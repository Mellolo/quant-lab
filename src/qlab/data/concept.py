"""ConceptClassification 查询工具 — PIT 安全."""

from __future__ import annotations

import pandas as pd


def concepts_as_of(
    concepts: pd.DataFrame,
    symbols: list[str],
    date: pd.Timestamp,
    source: str = "eastmoney",
) -> pd.DataFrame:
    """返回某日各 symbol 所属的有效概念列表.

    筛选逻辑：effective_date <= date 且 (expired_date > date 或 expired_date 为 NaT)。
    一个 symbol 可对应多行（多个概念）。

    返回
    ----
    DataFrame, columns=[symbol, concept_code, concept_name]
    """
    date = pd.Timestamp(date).normalize()

    df = concepts.reset_index() if isinstance(concepts.index, pd.MultiIndex) else concepts.copy()

    df = df[df["source"] == source]
    if df.empty:
        return pd.DataFrame(columns=["symbol", "concept_code", "concept_name"])

    df = df[df["symbol"].isin(symbols)]
    if df.empty:
        return pd.DataFrame(columns=["symbol", "concept_code", "concept_name"])

    mask_start = df["effective_date"] <= date
    mask_end = df["expired_date"].isna() | (df["expired_date"] > date)
    active = df[mask_start & mask_end]

    return active[["symbol", "concept_code", "concept_name"]].reset_index(drop=True)


def concept_members_as_of(
    concepts: pd.DataFrame,
    concept_code: str,
    date: pd.Timestamp,
    source: str = "eastmoney",
) -> list[str]:
    """返回某日某概念下的所有 symbol."""
    date = pd.Timestamp(date).normalize()

    df = concepts.reset_index() if isinstance(concepts.index, pd.MultiIndex) else concepts.copy()

    df = df[(df["source"] == source) & (df["concept_code"] == concept_code)]
    if df.empty:
        return []

    mask_start = df["effective_date"] <= date
    mask_end = df["expired_date"].isna() | (df["expired_date"] > date)
    active = df[mask_start & mask_end]

    return sorted(active["symbol"].unique().tolist())
