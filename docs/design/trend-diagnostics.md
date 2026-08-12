# 趋势诊断（Trend Diagnostics）

独立于采样门与因子库的日线趋势评价工具：回答「这段 K 线现在怎么走」。

## 定位

| 层 | 模块 | 职责 |
|----|------|------|
| 算法 | `qlab.core.price_panels` | 枢轴 / 结构状态 / Stage / smooth mom |
| 门面 | `qlab.diagnostics.trend` | `TrendReport` / `diagnose_trend` / `trend_panels` |
| 适配 | features / sample_masks | 可选：把诊断结果挂到因子或采样门 |

```text
OHLCV → price_panels → trend_panels / diagnose_trend → (feature | mask | notebook)
```

## 五轴

对每个确认日（收盘后）：

| 轴 | 取值 | 含义 |
|----|------|------|
| `direction` | -1 / 0 / +1 | 空 / 震荡 / 多（主：slow 结构；fast/slow 冲突则 0） |
| `strength` | [0,1] | 滚动分位合成（smooth mom + 结构扩展比） |
| `phase` | early / mid / late / range | 初 / 中 / 末 / 区间 |
| `quality` | [0,1] | R² + 回调健康度（+ 可选量能） |
| `risk` | [0,1] | CHoCH / Stage3 / 贴高点滞涨 / 冲突 |

另有 `regime` 字符串便于人读；研究与门控优先用分轴。

## 市场结构（孤立高低点）

- 枢轴：左右各 `L` 根（fast=`3`，slow=`5`）
- **确认日 = 枢轴右端收盘日**（PIT：此前不可用）
- 破位：收盘越过最近 swing high/low（不用影线）
- BOS：趋势方向上的收盘突破（同水平只计一次）
- CHoCH：反向收盘跌破保护摆动 → `phase=late`，方向清零

## 相位规则（默认）

- `range`：`direction=0`，或 Stage∈{1,3} 且 strength 低；fast/slow 冲突也倾向 range
- `early`：有方向，本段 `bos_count≤1`，且（多头）距 120 日高仍远
- `mid`：`bos_count≥2` 且 strength≥0.6
- `late`：CHoCH，或 Stage 2→3，或贴 60 日高且 strength 较 20 日前回落

## API

### 单标的

```python
from qlab.diagnostics import diagnose_trend

report = diagnose_trend(ohlcv_df)  # columns: close[, high, low, volume]
print(report.summary)
print(report.direction, report.phase, report.strength)
```

### 全市场宽表

```python
from qlab.diagnostics import trend_panels

panels = trend_panels(close, high=high, low=low, volume=volume)
# panels["direction"], ["strength"], ["phase_code"], ...
```

`phase_code`：`range=0, early=1, mid=2, late=3`（见 `PHASE_CODE`）。

## 默认参数

- Stage：`ma=200`, `slope_lookback=20`
- smooth mom：`window=60`；滚动分位窗 `252`
- dist_high：`60` / `120`
- 无 volume 时跳过量能项

## 与采样 / 因子

- 采样门：`trend_phase_mask` / `bull_trend_mask`（`sample_masks`）
- 因子：`TrendDirection` / `TrendStrength` / `TrendPhase`（`features.library.trend`）

诊断本体不依赖 labeling；适配器可按需启用。

## 本地产物

案例报告、图表等运行产物放在 gitignore 的 `output/<产物名>/`（例如 `output/trend-diag-600390/`），不进仓库。
