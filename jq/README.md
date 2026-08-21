# jq — 聚宽 Jupyter kernel 远程执行桥

通过聚宽（JoinQuant）研究环境（标准 JupyterHub）的 REST + WebSocket API，
在远程 kernel 里执行任意 Python 代码并取回输出。本质是"远程驱动聚宽 kernel
跑 jqdata，把结果文本捞回本地"——本地无需安装 jqdata。

## 安装

```bash
pip install -e ./jq
```

## 配置

1. **base_url**（聚宽研究环境地址，形如 `https://www.joinquant.com/user/<uid>`）：
   设环境变量 `JQ_BASE_URL`，或 CLI 传 `--base-url`。
2. **cookie**：从已登录 `joinquant.com` 的浏览器复制任意请求的完整 `Cookie` 头，
   执行：
   ```bash
   jqdata cookies 'user-...=...; _xsrf=...; token=...; PHPSESSID=...'
   ```
   cookie 存于 `~/.jq/cookies.json`（权限 0600）。JupyterHub 会话过期由连接器
   自动 OAuth 续期，常态零干预；仅当主站登录态也失效时才需重新登录 + 更新 cookie。

## 用法

```bash
jqdata test                                    # 测连接 + 列可用 kernel
jqdata kernel warmup                           # 预创建 kernel + 预加载 jqdata
jqdata run 'print(1+1)'                        # 跑代码
jqdata run --file my_analysis.py               # 从文件跑
jqdata run --file fetch.py --exec-timeout 40   # 长任务调大超时
jqdata kernel status                           # 查持久化 kernel 状态
jqdata kernel close                            # 关闭并清理 kernel
jqdata cache status                            # 查看缓存统计
jqdata cache clear                             # 清空当前布局版本的缓存
jqdata cache clear --older-than 30             # 仅清 30 天前的
jqdata cache clear --all-versions              # 连旧布局版本目录一起清
jqdata cache prune                             # 删除当月/未来月分片(不完整数据)
```

## 本地数据缓存 (`jq.cache`)

`DataCache` 在本地 (`<CWD>/.jqcache/`) 缓存聚宽取数结果, 避免重复远程查询.
可用环境变量 `JQ_CACHE_DIR` 覆盖缓存目录.

### 存储布局

```
.jqcache/v2/                                  # v2 = 布局版本, 格式变更时旧缓存自动失效
  bars/get_price/600519_XSHG/daily__post/     # 时序类: 标的 × 变体 × 月
    2024-01.pkl  2024-02.pkl  ...             # 一月一片
  snapshot/get_concepts/all.pkl               # 快照类: key-value
```

**按月分片**带来四个性质:

- 天然知道缺哪个月 → 只补查缺失月, 跨越式请求不会连带重取中间区间;
- 单月写入互不影响, 配合原子写 (`tmp` + `os.replace`) 并发/中断不会写坏缓存;
- 查过但无数据的月份也落盘(空月标记) → 上市前/退市后/长期停牌区间不再反复远程;
- **当月及未来月永不落盘**(数据不完整), 每次查询都会重取.

实测: 单标的 7 年 = 90 个分片 / 0.27 MB, 纯缓存读取 **0.008s**.

### 复权口径(重要)

行情缓存**统一存后复权 (`fq='post'`)**, `raw` 与前复权在本地按 `factor` 推导:

| 口径 | 价格 | 成交量 | `factor` 列 |
|---|---|---|---|
| `fq='post'` | 原样 | 原样 | `f` |
| `fq=None` | `post / f` | `post × f` | `1.0` |
| `fq='pre'` | `post / f_ref` | `post × f_ref` | `f / f_ref` |

这样做的原因: 后复权的历史值不随未来分红改变(PIT 稳定), 且自带完整 factor 序列.
于是:

- 缓存内容与请求的 `fq` 无关, **永久有效**;
- 任何 `pre_factor_ref_date` 都不会污染缓存 —— 前复权基准日只影响返回值;
- 同一 (标的, 日期) 的价格永远唯一, **消除了跨基准日拼接产生的价格跳变**.

> 聚宽 `get_price` 不传 `fields` 时只返回 6 个基础字段(**不含 `factor`**),
> 因此缓存层始终显式请求全 12 字段
> (`open/close/low/high/volume/money/factor/high_limit/low_limit/avg/pre_close/paused`).

### 时点必填（全局纪律）

**任何浮动终点都被禁止**。不管是名录类还是行情类，只要“缺省就取到最新”，
同一段代码就会在不同日子返回不同结果——回测里这正是未来函数的入口。
想取最新就**显式写出今天**。

```python
import datetime as dt
ASOF = str(dt.date.today())      # 研究脚本里把时点提成常量

# 行情类 —— end_date 总是必需
dc.get_price('600519.XSHG', '2024-01-01', '2024-06-30')
dc.get_price('600519.XSHG', count=250, end_date=ASOF)
dc.get_price('600519.XSHG', count=250)        # ✗ ValueError

# 名录/成分类 —— date 是第一个位置参数
dc.get_all_securities(ASOF, types=['stock'])
dc.get_security_info('600519.XSHG', ASOF)
dc.get_industries(ASOF)
dc.get_index_stocks('000300.XSHG', ASOF)
dc.get_index_weights('000300.XSHG', ASOF)   # weight 是百分数, 月度粒度
dc.get_industry_stocks('HY001', ASOF)
dc.get_industry('600519.XSHG', ASOF)
dc.get_concept('600519.XSHG', ASOF)
dc.get_billboard_list(ASOF)
dc.get_trade_days(ASOF, start_date='2024-01-01')
dc.get_factor_values(['600519.XSHG'], ['pe_ratio'], ASOF, start_date='2024-01-01')
dc.run_cached('get_fundamentals', key, code, '2024-12-31')   # today_key 必填

dc.get_all_securities()          # ✗ TypeError
```

缓存语义因此完全自洽，不需要任何 TTL 机制：
**历史时点**的结果是确定事实 → 永久缓存；**今天**的 → 不落盘。

> `run_cached` 的 `today_key` 特意不给默认值：它是任意查询的逃生舱，
> 若允许缺省，就会把“取到最新”的浮动结果当成永久事实写进缓存。

少数聚宽接口本身没有 `date` 参数，只能返回“当前状态”——
`get_concepts()` / `get_all_factors()` / `get_all_trade_days()`。
它们按**取数日**分片缓存（`asof-2024-06-03.pkl`）：当天内命中、跨天自动刷新，
历史快照保留下来形成名录的时点序列（可回溯“某概念是何时新增的”）。
它们不提供 `as_of` 参数：聚宽永远只返回当前目录，接受一个历史 `as_of`
会把当前数据伪装成历史数据，比缓存过期更危险。

> 需要个股在某历史时点的概念归属，用 `get_concept(security, date)`（支持 date）。

### 两类缓存策略

| | 时序类(按月分片) | 快照类(key-value) |
|---|---|---|
| 缓存粒度 | 标的 × 变体 × 月 | 参数全组合 |
| 命中方式 | 逐月命中, 只补缺失月 | 参数完全匹配 |
| 空结果 | 写空月标记(不再重查) | 不缓存 |
| 当天/未来 | 当月不落盘 | 不落盘 |

**时序类** (8 个): `get_price` / `get_price_batch` / `get_money_flow` /
`get_valuation` / `get_index_valuation` / `get_mtss` / `get_extras` / `get_call_auction`

**快照类** (14 个): `get_all_securities` / `get_security_info` /
`get_all_trade_days` / `get_trade_days` / `get_industries` / `get_industry_stocks` /
`get_industry` / `get_concepts` / `get_concept` / `get_billboard_list` /
`get_index_stocks` / `get_index_weights` / `get_factor_values` / `get_all_factors`

### Python API

```python
from jq.cache import DataCache

dc = DataCache()                      # 默认 CWD/.jqcache/, 远程超时 300s
dc = DataCache(exec_timeout=600)      # 长任务可放大超时

# 单只 —— 首次远程 + 落盘; 二次 0ms; 扩大区间只补缺失月
df = dc.get_price('600519.XSHG', '2024-01-01', '2024-06-30')
df = dc.get_price('600519.XSHG', count=250, end_date='2024-06-28')  # 最近 250 个交易日
df = dc.get_price('600519.XSHG', '2024-01-01', '2024-06-30', fq=None)   # 不复权

# 批量 —— 一次远程调用取多只, 分别落盘; 与单只共用同一份缓存
data = dc.get_price_batch(['600519.XSHG', '000001.XSHE'],
                          '2024-01-01', '2024-06-30', chunk_size=50)
df = dc.get_price('600519.XSHG', '2024-01-01', '2024-06-30')  # ← 直接命中

df = dc.get_valuation('600519.XSHG', '2024-01-01', '2024-06-30',
                      fields=['pe_ratio', 'pb_ratio'])
df = dc.get_index_valuation('000001.XSHG', '2024-01-01', '2024-06-30',
                            fields=['turnover_ratio', 'circulating_market_cap'])
# 中证全指用 000985.CSI, 不要用 000985.XSHG(空表)

# 快照类 —— 时点必填(见上文"必须显式绑定时点")
stocks = dc.get_index_stocks('000300.XSHG', '2024-06-03')
all_days = dc.get_all_trade_days()   # 无 date 参数, 按取数日分片

# run_cached — 适用于 query_object 类 (get_fundamentals / finance.run_query)
dc.run_cached(
    "get_fundamentals",
    cache_key=("600519", "2024q4", "pe"),
    user_code=(
        "from jqdata import *\n"
        "q = query(valuation.code, valuation.pe_ratio)"
        ".filter(valuation.code == '600519.XSHG')\n"
        "__result__ = get_fundamentals(q, statDate='2024q4')"
    ),
    today_key="2024-12-31",   # 必填: 这份结果对应的数据时点
)

# 缓存管理
dc.status()                     # 统计 dict(按 bars / snapshot 分组)
dc.clear()                      # 清当前版本
dc.clear(older_than_days=30)
dc.clear(all_versions=True)     # 连旧布局版本一起清
dc.prune_stale_months()         # 删当月/未来月分片
```

### CLI 管理

```bash
jqdata cache status                 # 查看缓存统计
jqdata cache clear                  # 清空当前版本缓存
jqdata cache clear --older-than 30  # 仅清 30 天前的
jqdata cache clear --all-versions   # 连旧布局版本一起清
jqdata cache prune                  # 删当月/未来月分片
```

### 注意事项

- `get_security_info` 返回 dict (聚宽 Security 对象无法跨环境 pickle, 远程转为 dict)
- `get_industry_stocks` / `get_industry` / `get_concept` 在 `jqdata.apis` 子模块中,
  需 `from jqdata.apis import *` (非 `from jqdata import *`)
- `get_valuation` 不传 fields 时聚宽会尝试查不存在的 `pcf_ratio2` 字段, cache 层
  显式传安全字段列表规避
- `get_index_valuation` 只接单只代码(传 list 聚宽只回第一只且不报错);
  中证全指用 `000985.CSI`, 深证综指/国证A指/`000985.XSHG` 实测空表
- `fields` 里出现不存在的字段会**直接报错**(而非静默丢列)
- `end_date` / `date` / `today_key` 一律必填, 不接受“取到最新”(见上文)
- `count` 与 `start_date` 二选一, 但两者都需配 `end_date`
- 缓存用 pickle 存储(pandas 原生, 零额外依赖), 只读取本引擎自己写入的文件

## 跨版本序列化 (`jq.serialize`)

聚宽研究环境是 Python 3.6 + 老版 pandas, 本地通常 3.10+ / pandas 2.x.
本模块统一处理这条边界: **远程侧先净化再 base64-pickle**, 本地从 stdout 标记区解码.

- 相比 `to_json(orient='table')`: 保留 dtype / 时区 / MultiIndex, 且支持
  非 DataFrame 结果(dict / list / ndarray);
- 净化会把老版专有的 `Int64Index` / `Float64Index` 降级为两版本都存在的类型,
  否则空 DataFrame 反序列化必抛 `ModuleNotFoundError: pandas.core.indexes.numeric`.

`DataCache` 与 `runner.run_dataframe` 共用此通道.

## API 参考

`jq.api` 模块记录了所有已实测验证的 jqdata 函数签名、入参、返回值与文档存根:

```python
import jq.api
help(jq.api.get_price)     # 查看单个函数签名与文档
```

它是**类型存根**, 函数不可调用 —— 直接调用会抛 `NotImplementedError`
(而非静默返回 `None`), 并提示改用 `DataCache` 或 `jqdata run`.
签名、docstring 与 IDE 提示均完整保留.

## 开发

```bash
pip install -e './jq[dev]'
cd jq && pytest          # 单元测试(纯本地, 不触网)
ruff check src tests
```
