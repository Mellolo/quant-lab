# 采样 × 因子：防未来函数约定

> 状态：现行契约（与 `SampleSpec` / `build_labeled_samples` 代码一致）

## 1. 角色拆分

| 角色 | 产出 | 谁负责 |
|------|------|--------|
| 采样器 `EventSampler` | **确认日** `(timestamp, symbol)` | 只判「条件是否成立」 |
| `SampleSpec` | **入场日**事件 + `entry_timing` + 标签 | 确认日→入场映射、出场 TB |
| 特征矩阵 | 按 `entry_timing` 对齐后的日频因子 | `available_at` × shift |
| 拼接 | 事件行上的 `X` | 仅 `attach_features_to_events` |

**禁止**：`sampler.sample_per_symbol` → `to_event_dataframe(entry_timing=open)` → 手拼因子。  
确认日收盘信息会漏进「当日开盘决策」。

## 2. 推荐用法（唯一研究入口）

```python
from qlab.labeling import (
    CUSUMFilter,
    EXIT_RESEARCH_DEFAULT,
    SampleSpec,
    build_labeled_samples,
)

spec = SampleSpec(
    entry=CUSUMFilter(h=0.05),
    exit=EXIT_RESEARCH_DEFAULT,   # TB 3:1 / v20
    # entry_at 默认 next_open；少数场景用 EntryAt.CONFIRM_CLOSE
)

out = build_labeled_samples(
    spec,
    close_wide,                   # 采样用价格宽表
    target=0.03,                  # 或 targets_from_panel(vol)
    features=["mom_5d", "ewm_vol_20d", "is_stage2_200d"],
    data=layer,
    universe=universe,
    date_range=("2023-01-03", "2024-11-29"),
    open=open_wide,               # next_open 入场必传
    label_prices=daily[["open", "close"]],
)

# out.events  — 入场日事件（含 entry_timing）
# out.labels  — 三重屏障标签
# out.X       — 与事件对齐、且在入场点前可知的因子
# y = out.labels["bin"]
```

内部固定顺序：

1. `SampleSpec` → 入场日 `events` + `labels`
2. `build_feature_matrix(entry_timing=spec.event_entry_timing)`（**不可改传**）
3. `attach_features_to_events(events, matrix)`（入场点不一致则抛 `PITViolationError`）

## 3. `entry_at` ↔ 特征对齐

| `SampleSpec.entry_at` | `spec.event_entry_timing` | 特征侧 |
|----------------------|---------------------------|--------|
| `next_open`（默认） | `open` | `today_close` / `next_open` 因子 shift +1；竞价类当日可用 |
| `confirm_close` | `close` | 收盘前可知因子当日可用；`next_open` 仍 +1 |

手写拆步时也必须：

```python
X_mat = build_feature_matrix(..., entry_timing=spec.event_entry_timing)
X = attach_features_to_events(events, X_mat)
```

## 4. 采样器还能直接用吗？

可以，但**只**作为：

- `SampleSpec.entry=...` 的实现；
- 或诊断脚本里看确认日分布。

诊断确认日→入场日：

```python
confirm = spec.confirmation_pairs(close)
entry = spec.sample_pairs(close)   # 已映射
```

不要对 `confirm` 直接 `to_event_dataframe(..., entry_timing="open")`。

## 5. 出场

第一版仅经典三重屏障 `ExitSettings(pt, sl, vertical_days)`。  
移动止盈 / 前高止盈等见 `qlab.labeling.exit` 模块文档中的扩展点，尚未实现。

## 6. 采样器 vs 采样门

| 角色 | 产出 | 叠法 |
|------|------|------|
| **采样器** | 确认日 `(timestamp, symbol)` | 选一种主触发（多触发则 OR 合并 pairs） |
| **采样门** | `(date, symbol)` bool | `filter_pairs` / `combine_masks` **AND** 叠加 |
| **入场** | `SampleSpec` | 默认次日开盘；门不改入场日 |

```text
采样器 → pairs → 门₁ ∧ 门₂ ∧ … → SampleSpec → TB 标签
```

### 6.1 主栈推荐（trading-books 流水线）

**触发（择一）**

| 采样器 | 用途 |
|--------|------|
| `NewHighBreakoutSampler` | 短窗新高突破（默认候选池） |
| `daily_event_pairs` | 横截面动量 / 每日可排序 |
| `CUSUMFilter` | AFML 事件采样、控制换手 |

**质量门（按需 AND）**

| 门 | 层 |
|----|-----|
| `tradable_hygiene_mask`（含可选 `min_close` / `min_avg_amount`） | 可交易卫生 |
| `liquidity_top_n_mask` / `tradable_hygiene_mask(min_avg_amount=…)` | 流动性（采样门，非宇宙） |
| `stage2_mask` | 趋势结构（Weinstein） |
| `bull_trend_mask` | 趋势诊断多头门（见 [trend-diagnostics.md](./trend-diagnostics.md)） |
| `relative_strength_mask`（`method="smooth"` 或外部 `score=`） | 领涨 |
| `near_high_mask` | 强势区（距高点带） |
| `industry_rs_mask` ∩ `industry_leader_mask` | 板块对齐 |
| `volume_confirm_mask` | 突破量能 |
| `anti_climax_mask` | 反高潮 |
| `market_breadth_mask`（可选指数 Stage2 / 涨停净占比） | 环境开关 |

卫生门结果可用 `mask_to_wide(...)` 作为其他门的 `eligible=`。

### 6.2 实验 / 辅触发（非默认）

`VolumeCUSUMFilter`、`RunSampler`、`EntropySampler`、`HMMTrendSampler` — 保留作研究探针；主路径优先用「价格触发 + 量能/Stage2/RS 门」。
