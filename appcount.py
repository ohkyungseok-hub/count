# app.py
# 여러 엑셀 파일 업로드 → 전부 병합 → 결제번호 기준 정렬 → 동일 로직으로
# 1) 결제번호별 매출총이익/마진율
# 2) 스토어별·일자별 매출총이익/마진율
# 3) 전체 합계
# 결과 엑셀 다운로드

import re
from io import BytesIO
from typing import Optional, Tuple, List

import numpy as np
import pandas as pd
import streamlit as st


# -----------------------------
# Helpers
# -----------------------------
def to_number(x) -> float:
    """엑셀 셀의 숫자/문자(쉼표 포함) → float 변환. 실패 시 0."""
    if pd.isna(x):
        return 0.0
    if isinstance(x, (int, float, np.integer, np.floating)):
        return float(x)
    s = str(x).strip()
    if s == "":
        return 0.0
    s = s.replace(",", "")
    # (1,234) 형태 음수 처리
    if re.match(r"^\(.*\)$", s):
        s = "-" + s[1:-1]
    try:
        return float(s)
    except Exception:
        return 0.0


def pick_shipping_col(df: pd.DataFrame) -> Optional[str]:
    """
    배송비 컬럼 우선순위:
    1) 총 배송 및 배달비(결제기준)  (결제번호당 1회 반영하기에 가장 적합)
    2) 실 배송 및 배달비(주문기준)
    """
    if "총 배송 및 배달비(결제기준)" in df.columns:
        return "총 배송 및 배달비(결제기준)"
    if "실 배송 및 배달비(주문기준)" in df.columns:
        return "실 배송 및 배달비(주문기준)"
    return None


def validate_columns(df: pd.DataFrame) -> Tuple[bool, list]:
    required = [
        "결제번호",
        "스토어명",
        "주문일시",
        "총 결제액",
        "매입가",
        "주문상품수량",
        "해당 아이템 개별상품할인액",
    ]
    missing = [c for c in required if c not in df.columns]
    ship_col = pick_shipping_col(df)
    if ship_col is None:
        missing.append("총 배송 및 배달비(결제기준) 또는 실 배송 및 배달비(주문기준)")
    return (len(missing) == 0, missing)


def load_and_merge_excels(files: List) -> pd.DataFrame:
    """
    업로드된 여러 엑셀을 읽어 병합(concat).
    컬럼 구성이 조금 달라도 concat 가능하도록 union 컬럼으로 맞춤.
    """
    frames = []
    errors = []

    for f in files:
        try:
            temp = pd.read_excel(f)
            temp["_source_file"] = getattr(f, "name", "uploaded.xlsx")
            frames.append(temp)
        except Exception as e:
            errors.append(f"{getattr(f, 'name', 'uploaded.xlsx')}: {e}")

    if errors:
        raise ValueError("엑셀 읽기 실패:\n" + "\n".join(errors))

    if not frames:
        raise ValueError("읽을 수 있는 엑셀 데이터가 없습니다.")

    merged = pd.concat(frames, ignore_index=True, sort=False)
    return merged


def compute_results(
    df: pd.DataFrame,
    miro_store_name: str = "미로상사",
    miro_shipping_fixed: float = 4000.0,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
    """
    로직:
    - 매출총이익 = 총 결제액 − (매입가×주문상품수량) − (개별상품할인액×주문상품수량) − 배송비
    - 결제번호 같은 건: 총 결제액/배송비는 결제번호당 1회
    - 미로상사: 배송비 4000 고정(결제번호당 1회)
    - 그 외: 엑셀 배송비 그대로(결제번호당 1회)
    - 마진율(%) = 매출총이익 / 총 결제액 * 100
    """
    work = df.copy()

    # 배송비 컬럼 선택
    ship_col = pick_shipping_col(work)
    if ship_col is None:
        raise ValueError("배송비 컬럼을 찾을 수 없습니다.")
    used_ship_col = ship_col

    # 숫자 변환
    num_cols = [
        "총 결제액",
        "매입가",
        "주문상품수량",
        "해당 아이템 개별상품할인액",
        "총 배송 및 배달비(결제기준)",
        "실 배송 및 배달비(주문기준)",
    ]
    for c in num_cols:
        if c in work.columns:
            work[c] = work[c].map(to_number)

    # 날짜
    work["주문일자"] = pd.to_datetime(work["주문일시"], errors="coerce").dt.date

    # 라인 단위 원가/할인(수량 반영)
    work["매입원가"] = work["매입가"] * work["주문상품수량"]
    work["할인(수량반영)"] = work["해당 아이템 개별상품할인액"] * work["주문상품수량"]

    # 결제번호별 집계 (중복 제거 핵심)
    def agg_payment(g: pd.DataFrame) -> pd.Series:
        store = str(g["스토어명"].iloc[0]) if len(g) else ""
        order_date = g["주문일자"].iloc[0] if len(g) else None

        total_payment = float(g["총 결제액"].iloc[0]) if len(g) else 0.0

        if store == miro_store_name:
            shipping = float(miro_shipping_fixed)
        else:
            shipping = float(g[used_ship_col].iloc[0]) if len(g) else 0.0

        cost_sum = float(g["매입원가"].sum())
        disc_sum = float(g["할인(수량반영)"].sum())

        profit = total_payment - cost_sum - disc_sum - shipping
        margin = (profit / total_payment * 100.0) if total_payment != 0 else np.nan

        return pd.Series(
            {
                "스토어명": store,
                "주문일자": order_date,
                "총 결제액(1회)": total_payment,
                "배송비(적용, 1회)": shipping,
                "매입원가합": cost_sum,
                "할인합(수량반영)": disc_sum,
                "매출총이익": profit,
                "마진율(%)": round(margin, 2) if pd.notna(margin) else np.nan,
            }
        )

    payment_result = (
        work.groupby("결제번호", dropna=False)
        .apply(agg_payment)
        .reset_index()
        # 요청: 결제번호 순으로 정렬
        .sort_values(["결제번호"], na_position="last")
        .reset_index(drop=True)
    )

    # 스토어별·일자별 (결제번호별 결과를 합산)
    store_date = (
        payment_result.groupby(["스토어명", "주문일자"], as_index=False)
        .agg({"매출총이익": "sum", "총 결제액(1회)": "sum"})
        .rename(columns={"총 결제액(1회)": "총결제액합"})
        .sort_values(["주문일자", "스토어명"], na_position="last")
        .reset_index(drop=True)
    )
    store_date["마진율(%)"] = np.where(
        store_date["총결제액합"] != 0,
        (store_date["매출총이익"] / store_date["총결제액합"] * 100.0).round(2),
        np.nan,
    )

    total_profit = float(payment_result["매출총이익"].sum())
    total_sales = float(payment_result["총 결제액(1회)"].sum())
    total_margin = (total_profit / total_sales * 100.0) if total_sales != 0 else np.nan

    total_df = pd.DataFrame(
        {
            "전체 총결제액합": [total_sales],
            "전체 매출총이익 합계": [total_profit],
            "전체 마진율(%)": [round(total_margin, 2) if pd.notna(total_margin) else np.nan],
        }
    )

    return payment_result, store_date, total_df, used_ship_col


def to_excel_bytes(
    payment_result: pd.DataFrame,
    store_date: pd.DataFrame,
    total_df: pd.DataFrame,
    merged_preview: Optional[pd.DataFrame] = None,
) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        payment_result.to_excel(writer, index=False, sheet_name="결제번호별")
        store_date.to_excel(writer, index=False, sheet_name="스토어별_일자별")
        total_df.to_excel(writer, index=False, sheet_name="전체합계")
        # 원하면 병합 원본 일부도 남김(디버깅/검증용)
        if merged_preview is not None:
            merged_preview.to_excel(writer, index=False, sheet_name="병합원본_미리보기")
    return output.getvalue()


# -----------------------------
# Streamlit UI
# -----------------------------
st.set_page_config(page_title="매출총이익 자동 계산기(다중 파일)", layout="wide")

st.title("매출총이익 자동 계산기 (복수 엑셀 병합 → 결제번호 정렬 → 자동 산출)")

with st.expander("계산 로직 보기", expanded=False):
    st.markdown(
        """
- **매출총이익 = 총 결제액 − (매입가 × 주문상품수량) − (해당 아이템 개별상품할인액 × 주문상품수량) − 실 배송 및 배달비**
- **결제번호가 같은 주문**: `총 결제액`, `배송/배달비`는 **결제번호당 1회만 반영**
- **미로상사**: 배송/배달비를 **무조건 4,000원**으로 적용(결제번호당 1회)
- **그 외 스토어**: 엑셀에 기입된 배송/배달비 금액 그대로 사용(결제번호당 1회)
- **마진율(%) = 매출총이익 / 총 결제액 × 100**
- 업로드한 **여러 엑셀을 모두 병합(concat)** 후 계산합니다.
"""
    )

left, right = st.columns([1, 1])

with left:
    uploaded_files = st.file_uploader(
        "주문내역 엑셀(.xlsx)을 여러 개 업로드하세요",
        type=["xlsx"],
        accept_multiple_files=True,
    )

with right:
    miro_store_name = st.text_input("미로상사 스토어명", value="미로상사")
    miro_shipping_fixed = st.number_input("미로상사 배송비 고정값", min_value=0, value=4000, step=100)
    show_merged_preview = st.checkbox("결과 엑셀에 병합 원본 미리보기 시트 포함", value=False)

if not uploaded_files:
    st.info("엑셀 파일을 1개 이상 업로드하면 결과가 생성됩니다.")
    st.stop()

# Load & merge
try:
    merged_df = load_and_merge_excels(uploaded_files)
except Exception as e:
    st.error(str(e))
    st.stop()

ok, missing = validate_columns(merged_df)
if not ok:
    st.error("필수 컬럼이 누락되어 계산할 수 없습니다. (여러 파일 병합 결과 기준)")
    st.write("누락 컬럼:", missing)
    st.stop()

# Compute
try:
    payment_result, store_date_result, total_df, used_ship_col = compute_results(
        merged_df,
        miro_store_name=miro_store_name,
        miro_shipping_fixed=float(miro_shipping_fixed),
    )
except Exception as e:
    st.error(f"계산 중 오류가 발생했습니다: {e}")
    st.stop()

st.caption(
    f"업로드 파일 수: **{len(uploaded_files)}개** | 병합 행 수: **{len(merged_df):,}행** | "
    f"배송비 컬럼 사용: **{used_ship_col}** | 미로상사 고정 배송비: **{miro_shipping_fixed:,.0f}원**"
)

# Metrics
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("결제번호 건수", f"{payment_result['결제번호'].nunique():,}")
with c2:
    st.metric("스토어 수", f"{payment_result['스토어명'].nunique():,}")
with c3:
    st.metric("전체 매출총이익 합계", f"{total_df.loc[0, '전체 매출총이익 합계']:,.0f}")
with c4:
    mr = total_df.loc[0, "전체 마진율(%)"]
    st.metric("전체 마진율(%)", "-" if pd.isna(mr) else f"{mr:.2f}%")

# Tables
st.subheader("결제번호별 (결제번호 순 정렬)")
st.dataframe(payment_result, use_container_width=True, hide_index=True)

st.subheader("스토어별 · 일자별")
st.dataframe(store_date_result, use_container_width=True, hide_index=True)

st.subheader("전체합계")
st.dataframe(total_df, use_container_width=True, hide_index=True)

# Download
preview_df = merged_df.head(2000) if show_merged_preview else None
xlsx_bytes = to_excel_bytes(payment_result, store_date_result, total_df, merged_preview=preview_df)

st.download_button(
    label="결과 엑셀 다운로드",
    data=xlsx_bytes,
    file_name="매출총이익_결과(병합).xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)
