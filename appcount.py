# app.py
# ✅ 요구사항 반영 완성본
#
# 1) 여러 엑셀 업로드 → 병합
# 2) 결제번호 기준
#    - 총 결제액: 결제번호당 1회
#    - 매입원가: 라인 합산 (매입가 * 주문상품수량)
#    - 할인: 라인 합산 (해당 아이템 개별상품할인액 * 주문상품수량)
#    - 배송비: 결제번호당 1회 (엑셀 값 사용)
#      * 단, 결제번호에 '미로상사'가 포함되어 있고 AND 엑셀 배송비가 0/빈값이면 추가로 4000원 더 차감
# 3) 복수 스토어 결제번호일 때:
#    - 총 결제액을 '실 주문상품액(결제기준)'(없으면 주문기준, 없으면 균등) 비율로 스토어별 안분
#    - 배송비(결제번호 1회)도 동일 비율로 스토어별 안분
#    - 스토어별 매출총이익 = 안분결제액 − (스토어 원가/할인/안분배송비)
# 4) 결과 엑셀 시트:
#    - 결제번호별
#    - 결제번호-스토어별(안분 상세)
#    - 스토어별_일자별
#    - 전체합계

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
    """숫자/문자(쉼표 포함, ( ) 음수) → float. 실패 시 0."""
    if pd.isna(x):
        return 0.0
    if isinstance(x, (int, float, np.integer, np.floating)):
        return float(x)
    s = str(x).strip()
    if s == "":
        return 0.0
    s = s.replace(",", "")
    if re.match(r"^\(.*\)$", s):
        s = "-" + s[1:-1]
    try:
        return float(s)
    except Exception:
        return 0.0


def pick_shipping_col(df: pd.DataFrame) -> Optional[str]:
    """배송비 컬럼 우선순위"""
    if "총 배송 및 배달비(결제기준)" in df.columns:
        return "총 배송 및 배달비(결제기준)"
    if "실 배송 및 배달비(주문기준)" in df.columns:
        return "실 배송 및 배달비(주문기준)"
    return None


def pick_alloc_basis_col(df: pd.DataFrame) -> Optional[str]:
    """결제액 안분 기준 컬럼 우선순위"""
    if "실 주문상품액(결제기준)" in df.columns:
        return "실 주문상품액(결제기준)"
    if "실 주문상품액(주문기준)" in df.columns:
        return "실 주문상품액(주문기준)"
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
    if pick_shipping_col(df) is None:
        missing.append("총 배송 및 배달비(결제기준) 또는 실 배송 및 배달비(주문기준)")
    return (len(missing) == 0, missing)


def load_and_merge_excels(files: List) -> pd.DataFrame:
    frames, errors = [], []
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
    return pd.concat(frames, ignore_index=True, sort=False)


# -----------------------------
# Core compute
# -----------------------------
def compute_results(
    df: pd.DataFrame,
    miro_store_name: str = "미로상사",
    miro_extra_shipping: float = 4000.0,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, str, Optional[str]]:
    work = df.copy()

    ship_col = pick_shipping_col(work)
    if ship_col is None:
        raise ValueError("배송비 컬럼을 찾을 수 없습니다.")
    used_ship_col = ship_col

    basis_col = pick_alloc_basis_col(work)

    # 숫자 변환
    num_cols = [
        "총 결제액",
        "매입가",
        "주문상품수량",
        "해당 아이템 개별상품할인액",
        "총 배송 및 배달비(결제기준)",
        "실 배송 및 배달비(주문기준)",
        "실 주문상품액(결제기준)",
        "실 주문상품액(주문기준)",
    ]
    for c in num_cols:
        if c in work.columns:
            work[c] = work[c].map(to_number)

    # 날짜
    work["주문일자"] = pd.to_datetime(work["주문일시"], errors="coerce").dt.date

    # 라인 원가/할인(수량 반영)
    work["매입원가"] = work["매입가"] * work["주문상품수량"]
    work["할인(수량반영)"] = work["해당 아이템 개별상품할인액"] * work["주문상품수량"]

    # 결제번호당 총 결제액 1회
    pay_total = (
        work.groupby("결제번호", dropna=False)["총 결제액"]
        .first()
        .reset_index()
        .rename(columns={"총 결제액": "총 결제액(1회)"})
    )

    # 결제번호당 엑셀 배송비 1회(원본)
    pay_ship_raw = (
        work.groupby("결제번호", dropna=False)[used_ship_col]
        .first()
        .reset_index()
        .rename(columns={used_ship_col: "배송비_원본(결제번호 1회)"})
    )

    # 결제번호에 미로상사 포함 여부
    has_miro = (
        work.assign(_is_miro=work["스토어명"].astype(str).eq(miro_store_name))
        .groupby("결제번호", dropna=False)["_is_miro"]
        .any()
        .reset_index()
        .rename(columns={"_is_miro": "미로상사포함"})
    )

    # ✅ 최종 배송비(결제번호 1회)
    # - 기본: 엑셀 배송비(1회)
    # - 추가: 미로상사 포함 & 엑셀 배송비가 0/빈값이면 4000 추가 차감
    pay_ship = pay_ship_raw.merge(has_miro, on="결제번호", how="left")
    pay_ship["배송비_원본(결제번호 1회)"] = pay_ship["배송비_원본(결제번호 1회)"].fillna(0.0).astype(float)
    pay_ship["미로상사포함"] = pay_ship["미로상사포함"].fillna(False)

    pay_ship["추가차감(미로상사&배송비0)"] = np.where(
        (pay_ship["미로상사포함"]) & (pay_ship["배송비_원본(결제번호 1회)"] == 0),
        float(miro_extra_shipping),
        0.0,
    )
    pay_ship["배송비(적용, 결제번호 1회)"] = (
        pay_ship["배송비_원본(결제번호 1회)"] + pay_ship["추가차감(미로상사&배송비0)"]
    )

    # 결제번호-스토어 단위 집계(원가/할인/안분기준)
    agg_map = {
        "주문일자": "min",
        "매입원가": "sum",
        "할인(수량반영)": "sum",
    }
    if basis_col is not None:
        agg_map[basis_col] = "sum"

    store_level = (
        work.groupby(["결제번호", "스토어명"], dropna=False)
        .agg(agg_map)
        .reset_index()
    )

    # 결제번호별 총결제액/배송비 붙이기
    store_alloc = (
        store_level
        .merge(pay_total, on="결제번호", how="left")
        .merge(
            pay_ship[
                [
                    "결제번호",
                    "배송비_원본(결제번호 1회)",
                    "추가차감(미로상사&배송비0)",
                    "배송비(적용, 결제번호 1회)",
                    "미로상사포함",
                ]
            ],
            on="결제번호",
            how="left",
        )
    )

    # 안분 비율(결제액 기준)
    if basis_col is not None:
        basis_sum = (
            store_alloc.groupby("결제번호", dropna=False)[basis_col]
            .sum()
            .reset_index()
            .rename(columns={basis_col: "_basis_sum"})
        )
        store_alloc = store_alloc.merge(basis_sum, on="결제번호", how="left")
        store_alloc["안분비율"] = np.where(
            store_alloc["_basis_sum"] != 0,
            store_alloc[basis_col] / store_alloc["_basis_sum"],
            np.nan,
        )
    else:
        # 안분기준 없으면 스토어 균등 안분
        store_counts = (
            store_alloc.groupby("결제번호", dropna=False)["스토어명"]
            .nunique()
            .reset_index()
            .rename(columns={"스토어명": "_store_cnt"})
        )
        store_alloc = store_alloc.merge(store_counts, on="결제번호", how="left")
        store_alloc["안분비율"] = np.where(
            store_alloc["_store_cnt"] != 0,
            1.0 / store_alloc["_store_cnt"],
            np.nan,
        )

    store_alloc["안분비율"] = store_alloc["안분비율"].fillna(0.0)

    # 결제액/배송비 안분
    store_alloc["안분결제액"] = store_alloc["총 결제액(1회)"].fillna(0.0) * store_alloc["안분비율"]
    store_alloc["안분배송비"] = store_alloc["배송비(적용, 결제번호 1회)"].fillna(0.0) * store_alloc["안분비율"]

    # 스토어별 이익/마진
    store_alloc["스토어별_매출총이익"] = (
        store_alloc["안분결제액"].fillna(0.0)
        - store_alloc["매입원가"].fillna(0.0)
        - store_alloc["할인(수량반영)"].fillna(0.0)
        - store_alloc["안분배송비"].fillna(0.0)
    )
    store_alloc["스토어별_마진율(%)"] = np.where(
        store_alloc["안분결제액"] != 0,
        (store_alloc["스토어별_매출총이익"] / store_alloc["안분결제액"] * 100.0).round(2),
        np.nan,
    )

    # 결제번호별 결과(결제번호당 배송비 1회)
    payment_result = (
        store_alloc.groupby("결제번호", dropna=False)
        .agg(
            주문일자=("주문일자", "min"),
            총결제액_1회=("총 결제액(1회)", "first"),
            배송비원본_1회=("배송비_원본(결제번호 1회)", "first"),
            미로상사추가차감=("추가차감(미로상사&배송비0)", "first"),
            배송비적용_1회=("배송비(적용, 결제번호 1회)", "first"),
            미로상사포함=("미로상사포함", "first"),
            매입원가합=("매입원가", "sum"),
            할인합_수량반영=("할인(수량반영)", "sum"),
            스토어수=("스토어명", "nunique"),
        )
        .reset_index()
    )

    payment_result = payment_result.rename(
        columns={
            "총결제액_1회": "총 결제액(1회)",
            "배송비적용_1회": "배송비(적용, 1회)",
        }
    )

    payment_result["매출총이익"] = (
        payment_result["총 결제액(1회)"].fillna(0.0)
        - payment_result["매입원가합"].fillna(0.0)
        - payment_result["할인합_수량반영"].fillna(0.0)
        - payment_result["배송비(적용, 1회)"].fillna(0.0)
    )
    payment_result["마진율(%)"] = np.where(
        payment_result["총 결제액(1회)"] != 0,
        (payment_result["매출총이익"] / payment_result["총 결제액(1회)"] * 100.0).round(2),
        np.nan,
    )

    payment_result = payment_result.sort_values("결제번호", na_position="last").reset_index(drop=True)

    # 결제번호-스토어별(안분 상세)
    detail_cols = [
        "결제번호",
        "스토어명",
        "주문일자",
        "총 결제액(1회)",
        "배송비_원본(결제번호 1회)",
        "추가차감(미로상사&배송비0)",
        "배송비(적용, 결제번호 1회)",
        "안분비율",
        "안분결제액",
        "안분배송비",
        "매입원가",
        "할인(수량반영)",
    ]
    if basis_col is not None:
        # 안분 기준 컬럼을 안분비율 앞쪽에 넣어 검증 용이하게
        insert_pos = detail_cols.index("안분비율")
        detail_cols.insert(insert_pos, basis_col)

    store_payment_detail = store_alloc[detail_cols + ["스토어별_매출총이익", "스토어별_마진율(%)"]].copy()
    store_payment_detail = store_payment_detail.sort_values(["결제번호", "스토어명"], na_position="last").reset_index(drop=True)

    # 스토어별 · 일자별 (안분 포함)
    store_date_result = (
        store_payment_detail.groupby(["스토어명", "주문일자"], as_index=False)
        .agg(
            안분결제액합=("안분결제액", "sum"),
            매출총이익=("스토어별_매출총이익", "sum"),
        )
        .sort_values(["주문일자", "스토어명"], na_position="last")
        .reset_index(drop=True)
    )
    store_date_result["마진율(%)"] = np.where(
        store_date_result["안분결제액합"] != 0,
        (store_date_result["매출총이익"] / store_date_result["안분결제액합"] * 100.0).round(2),
        np.nan,
    )

    # 전체 합계
    total_profit = float(payment_result["매출총이익"].sum())
    total_sales = float(payment_result["총 결제액(1회)"].sum())
    total_margin = (total_profit / total_sales * 100.0) if total_sales != 0 else np.nan

    total_df = pd.DataFrame(
        {
            "전체 총결제액합": [total_sales],
            "전체 매출총이익 합계": [total_profit],
            "전체 마진율(%)": [round(total_margin, 2) if pd.notna(total_margin) else np.nan],
            "배송비 규칙": [f"결제번호 1회(엑셀값) + ({miro_store_name} 포함 & 배송비=0이면 {miro_extra_shipping:,.0f} 추가차감)"],
            "결제액 안분 기준컬럼": [basis_col if basis_col is not None else "없음(스토어 균등 안분)"],
            "배송비 컬럼 사용": [used_ship_col],
        }
    )

    return payment_result, store_payment_detail, store_date_result, total_df, used_ship_col, basis_col


def to_excel_bytes(
    payment_result: pd.DataFrame,
    store_payment_detail: pd.DataFrame,
    store_date_result: pd.DataFrame,
    total_df: pd.DataFrame,
    merged_preview: Optional[pd.DataFrame] = None,
) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        payment_result.to_excel(writer, index=False, sheet_name="결제번호별")
        store_payment_detail.to_excel(writer, index=False, sheet_name="결제번호-스토어별")
        store_date_result.to_excel(writer, index=False, sheet_name="스토어별_일자별")
        total_df.to_excel(writer, index=False, sheet_name="전체합계")
        if merged_preview is not None:
            merged_preview.to_excel(writer, index=False, sheet_name="병합원본_미리보기")
    return output.getvalue()


# -----------------------------
# Streamlit UI
# -----------------------------
st.set_page_config(page_title="매출총이익 자동 계산기(다중 파일+안분)", layout="wide")
st.title("매출총이익 자동 계산기 (복수 엑셀 병합 + 안분 + 배송비 예외)")

with st.expander("현재 적용 로직(요약)", expanded=False):
    st.markdown(
        """
- **매출총이익 = 총 결제액 − (매입가×수량) − (개별상품할인액×수량) − 배송비**
- **총 결제액**: 결제번호당 1회
- **배송비**: 결제번호당 1회(엑셀값)
  - 단, **결제번호에 미로상사가 포함되어 있고 & 엑셀 배송비가 0이면** 추가로 **4,000원** 차감
- **복수 스토어 결제번호**:
  - 총 결제액을 **실 주문상품액(결제기준)** 비율로 스토어별 안분(없으면 주문기준, 없으면 균등)
  - 배송비도 동일 비율로 안분
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
    miro_extra_shipping = st.number_input(
        "미로상사 포함 & 배송비 0일 때 추가 차감액",
        min_value=0,
        value=4000,
        step=100,
    )
    show_merged_preview = st.checkbox("결과 엑셀에 병합 원본 미리보기 시트 포함", value=False)

if not uploaded_files:
    st.info("엑셀 파일을 1개 이상 업로드하면 결과가 생성됩니다.")
    st.stop()

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

try:
    payment_result, store_payment_detail, store_date_result, total_df, used_ship_col, basis_col = compute_results(
        merged_df,
        miro_store_name=miro_store_name,
        miro_extra_shipping=float(miro_extra_shipping),
    )
except Exception as e:
    st.error(f"계산 중 오류가 발생했습니다: {e}")
    st.stop()

st.caption(
    f"업로드 파일 수: **{len(uploaded_files)}개** | 병합 행 수: **{len(merged_df):,}행** | "
    f"배송비 컬럼: **{used_ship_col}** | 안분 기준: **{basis_col if basis_col else '없음(스토어 균등 안분)'}**"
)

# Metrics
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("결제번호 건수", f"{payment_result['결제번호'].nunique():,}")
with c2:
    st.metric("스토어 수(안분 상세)", f"{store_payment_detail['스토어명'].nunique():,}")
with c3:
    st.metric("전체 매출총이익 합계", f"{total_df.loc[0, '전체 매출총이익 합계']:,.0f}")
with c4:
    mr = total_df.loc[0, "전체 마진율(%)"]
    st.metric("전체 마진율(%)", "-" if pd.isna(mr) else f"{mr:.2f}%")

st.subheader("결제번호별 (결제번호 순 정렬)")
st.dataframe(payment_result, use_container_width=True, hide_index=True)

st.subheader("결제번호-스토어별 (안분 상세 / 검증용)")
st.dataframe(store_payment_detail, use_container_width=True, hide_index=True)

st.subheader("스토어별 · 일자별 (안분 포함)")
st.dataframe(store_date_result, use_container_width=True, hide_index=True)

st.subheader("전체합계")
st.dataframe(total_df, use_container_width=True, hide_index=True)

preview_df = merged_df.head(2000) if show_merged_preview else None
xlsx_bytes = to_excel_bytes(payment_result, store_payment_detail, store_date_result, total_df, merged_preview=preview_df)

st.download_button(
    label="결과 엑셀 다운로드",
    data=xlsx_bytes,
    file_name="매출총이익_결과(병합+안분+배송비예외).xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)
