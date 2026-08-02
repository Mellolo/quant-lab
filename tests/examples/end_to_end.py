"""端到端示例 — 从数据到回测的完整流水线.

用 FakeDataSource 演示，不需要任何外部数据源接入。
"""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd

from qlab.allocation import HierarchicalRiskParity, inverse_variance_portfolio
from qlab.data import DataLayer
from qlab.data.sources import FakeDataSource
from qlab.evaluation.statistics import (
    annualized_sharpe,
    compute_dd_tuw,
    deflated_sharpe_ratio,
    probabilistic_sharpe_ratio,
)
from qlab.features import build_feature_matrix
from qlab.features.library import EwmVol, Momentum, RealizedVol  # 触发注册
from qlab.labeling import CUSUMFilter, TripleBarrier, label_events
from qlab.models.cv.score import cv_score
from qlab.models.ensemble import build_bagging_classifier
from qlab.models.feature_importance import feat_imp_mdi
from qlab.sizing import bet_size_from_probability
from qlab.weights import sample_weights

warnings.filterwarnings("ignore")


def main():
    print("=" * 70)
    print("Quant Lab — 端到端示例")
    print("=" * 70)

    # ========================================================
    # 1. 数据层
    # ========================================================
    print("\n[1/8] 初始化数据层 (FakeDataSource)...")
    source = FakeDataSource(seed=42, n_symbols=20, start_year=2020)
    data = DataLayer(source=source)

    universe = data.universe("csi500", "2022-01-01", "2024-06-30")
    print(f"  Universe: {universe!r}")
    symbols = universe.all_symbols()[:10]  # 取前 10 只
    print(f"  使用 {len(symbols)} 只标的")

    # ========================================================
    # 2. 特征矩阵
    # ========================================================
    print("\n[2/8] 构建特征矩阵...")
    features = [
        Momentum(5),
        Momentum(20),
        EwmVol(20),
        RealizedVol(20),
    ]
    X = build_feature_matrix(
        features=features,
        data=data,
        universe=universe,
        date_range=("2022-01-01", "2024-06-30"),
    )
    print(f"  {X!r}")
    print(f"  前 5 行:\n{X.values.head()}")

    # ========================================================
    # 3. 事件采样 + 标注
    # ========================================================
    print("\n[3/8] CUSUM 事件采样 + Triple-Barrier 标注...")
    daily = data.daily(symbols, "2022-01-01", "2024-06-30", validate=False)
    close_panel = daily["close"].unstack("symbol")

    # 用日收益的 2 倍 std 作为 CUSUM 阈值
    log_ret = np.log(close_panel).diff()
    std_h = float(log_ret.stack().std()) * 2

    sampler = CUSUMFilter(h=std_h)
    event_pairs = sampler.sample_per_symbol(close_panel)
    print(f"  采样到 {len(event_pairs)} 个事件")

    if len(event_pairs) == 0:
        print("  无事件，退出示例")
        return

    # 构造 Event DataFrame
    cal = data.calendar
    events_list = []
    for _, row in event_pairs.iterrows():
        ts = row["timestamp"]
        sym = row["symbol"]
        try:
            t1 = cal.next_trading_day(ts, 7)  # 7 天垂直屏障
        except Exception:
            continue
        # 用该 symbol 当时的 EWM vol 作为 target
        try:
            vol = X.values.loc[(ts, sym), "ewm_vol_20d"]
            if pd.isna(vol) or vol <= 0:
                vol = 0.02
        except KeyError:
            vol = 0.02
        events_list.append({
            "event_start": ts, "symbol": sym, "t1": t1, "target": float(vol),
        })

    events = pd.DataFrame(events_list).set_index("event_start")
    print(f"  有效 events: {len(events)}")

    # 标注
    barrier = TripleBarrier(pt=2.0, sl=1.0)
    labels = label_events(events, daily, barrier)
    print(f"  标注完成: bin distribution = {labels['bin'].value_counts().to_dict()}")

    # ========================================================
    # 4. 样本权重
    # ========================================================
    print("\n[4/8] 计算样本权重 (uniqueness + return attr + time decay)...")
    weights = sample_weights(labels, daily, time_decay=0.5)
    print(f"  weight stats: mean={weights['final_weight'].mean():.3f}, "
          f"std={weights['final_weight'].std():.3f}")

    # ========================================================
    # 5. 模型训练 (PurgedKFold + Bagging + sample_weight)
    # ========================================================
    print("\n[5/8] 模型训练 (PurgedKFold + BaggingClassifier)...")
    # events/labels/weights 索引是单层 event_start, 同一时刻可能有多个 symbol
    # 把它们按位置上升到 (event_start, symbol) MultiIndex, 再与 X.values 做交集
    event_pairs = list(zip(events.index, events["symbol"], strict=False))
    train_mi = pd.MultiIndex.from_tuples(event_pairs, names=["date", "symbol"])

    # 用 numpy 位置索引保持 events/labels/weights 的行序与 train_mi 一致
    Xt_full = X.values.reindex(train_mi)
    # 仅保留 features 全部非 NaN 的样本
    valid_mask = Xt_full.notna().all(axis=1)
    if valid_mask.sum() < 50:
        print(f"  样本太少 ({int(valid_mask.sum())})，跳过模型训练")
        return

    Xt = Xt_full[valid_mask]
    keep_pos = valid_mask.values

    y_full = labels["bin"].values
    w_full = weights["final_weight"].values
    t1_full = events["t1"].values

    y = pd.Series(y_full[keep_pos], index=Xt.index)
    w = pd.Series(w_full[keep_pos], index=Xt.index)
    t1 = pd.Series(t1_full[keep_pos], index=Xt.index)

    if y.nunique() < 2:
        print("  标签退化为单类，跳过模型训练")
        return

    clf = build_bagging_classifier(n_estimators=50, max_samples=0.8, n_jobs=1)

    # CV
    scores = cv_score(
        clf, Xt, y, sample_weight=w, scoring="accuracy",
        t1=t1, cv=3, pct_embargo=0.01,
    )
    print(f"  CV accuracy: {scores.mean():.3f} ± {scores.std():.3f}")

    # 全量训练 + 特征重要性
    clf.fit(Xt.values, y.values, sample_weight=w.values)
    imp = feat_imp_mdi(clf, list(Xt.columns))
    print(f"  特征重要性（MDI）:\n{imp.round(3)}")

    # ========================================================
    # 6. 仓位 sizing
    # ========================================================
    print("\n[6/8] 仓位分配（概率 → bet size）...")
    probs = clf.predict_proba(Xt.values)
    max_prob = probs.max(axis=1)
    pred = clf.classes_[probs.argmax(axis=1)]
    bet_sizes = bet_size_from_probability(
        pd.Series(max_prob, index=Xt.index),
        pd.Series(pred, index=Xt.index),
        num_classes=len(clf.classes_),
    )
    print(f"  bet size 分布: mean={bet_sizes.mean():.3f}, "
          f"abs mean={bet_sizes.abs().mean():.3f}")

    # ========================================================
    # 7. 回测统计
    # ========================================================
    print("\n[7/8] 回测统计...")
    # 每个事件的 (bet_size × realized return) 当作单笔 PnL
    realized = pd.Series(labels["ret"].values[keep_pos], index=Xt.index)
    event_pnl = (bet_sizes * realized).dropna()

    # NAV 是时间序列, 需按日聚合（同一天的多个事件 PnL 取算术平均, 即等权组合）
    if len(event_pnl) > 5:
        daily_pnl = event_pnl.groupby(level="date").mean().sort_index()

        psr = probabilistic_sharpe_ratio(daily_pnl)
        ann_sr = annualized_sharpe(daily_pnl, periods_per_year=int(252 / 7))  # 周频
        print(f"  Annualized SR: {ann_sr:.3f}")
        print(f"  PSR: {psr:.3f}")

        # NAV + DD
        nav = (1 + daily_pnl).cumprod()
        dd, tuw = compute_dd_tuw(nav)
        if not dd.empty:
            print(f"  Max DD: {dd.max():.3%}")

        # DSR 假设有 50 次试验
        dsr = deflated_sharpe_ratio(daily_pnl, n_trials=50, var_trials_sr=0.5)
        print(f"  DSR (假设 50 trials, var=0.5): {dsr:.3f}")

    # ========================================================
    # 8. HRP 配置（演示）
    # ========================================================
    print("\n[8/8] HRP 资产配置...")
    returns = close_panel.pct_change().dropna()
    if returns.shape[1] >= 3:
        hrp = HierarchicalRiskParity()
        hrp_weights = hrp.allocate(returns)
        print(f"  HRP 权重（前 5）:\n{hrp_weights.head().round(4)}")

        ivp = inverse_variance_portfolio(returns.cov().values)
        print(f"  IVP 权重均值: {ivp.mean():.4f}")

    print("\n" + "=" * 70)
    print("端到端示例运行完成 ✓")
    print("=" * 70)


if __name__ == "__main__":
    main()
