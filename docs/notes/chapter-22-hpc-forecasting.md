# 第 22 章：高性能计算与预测技术（HPC and Forecasting）

> 客座章节，作者：**Kesheng Wu 与 Horst D. Simon**（Lawrence Berkeley National Laboratory）。介绍 LBNL 的 CIFT 项目（Computational Intelligence and Forecasting Technologies）——把国家实验室的 HPC 技术迁移到金融场景。**2010 Flash Crash 调查耗时 5 个月就因为数据量大**——HPC 几分钟就能处理同样量级。

## 一、动机：从 Flash Crash 到 CIFT

2010-05-06，道琼斯急跌近 10% 后几分钟内反弹。SEC + CFTC 花了 **5 个月** 才出调查报告，**理由就是数据量大（~20 TB）**。

LBNL 反应：**NERSC（国家能源研究科学计算中心）日常处理数百 TB 只要几分钟**。CIFT 项目由此诞生，把 HPC 技术迁移到金融数据分析。

CIFT 已应用方向：交通流、电网、电力消耗、金融。

## 二、HPC vs 云计算

| 维度 | HPC | 云 |
|---|---|---|
| 主要应用 | 大规模科学模拟 | 数据并行任务 |
| 储存结构 | 集中式，全局文件系统 | 分布式，靠近 CPU |
| 网络要求 | 高（InfiniBand） | 中（Ethernet 即可） |
| 虚拟化 | 几乎无 | 全栈虚拟化 |
| 重要软件 | MPI, HDF5, ADIOS | Hadoop / Spark |
| 流式分析适配 | **更好** | 较差 |
| 成本（同等任务） | 1× | 3-7× |

**为什么云对金融流式分析不友好**：云为高吞吐设计，不为实时响应。但 flash crash 等场景需要分钟级响应。

虚拟化代价：测试显示商业云上跑科学软件比 NERSC 慢 2-10 倍；高度并行时尤其严重（PARATEC 慢 53 倍）。**网络虚拟化是主因**——多核并行时几乎不 scale。

## 三、HPC 关键软件

### 3.1 MPI（Message Passing Interface）
并行计算的通信协议标准。**HPC 应用自己做进程间通信都靠它**。MPICH 是 Argonne 的开源实现，被各 HPC 厂商定制使用。LIS（语言无关规范）+ 多语言绑定让它跨语言广泛使用。

### 3.2 HDF5（Hierarchical Data Format 5）
HPC 标配的 I/O 库，把数据看作**多维数组 + 元属性 + 维度 + 数据类型 = data set**。datasets 可分组，组可嵌套。
- NASA 有 HDF5-EOS（地球观测）
- 生物信息有 BioHDF（DNA 测序）
- **金融**：把股票数据存为 HDF5 显著加速分析（efficient compression/decompression 减少网络流量和 I/O）

### 3.3 ADIOS（Adaptable I/O System）+ In Situ Processing
CPU 性能 18 月翻一倍（Moore's Law），磁盘性能 1 年才涨 5% → 写出 CPU 内存越来越慢。

ADIOS 解决方案：**在数据流入磁盘前就处理**（in-situ / in-flight analysis）。可丢弃无关数据，避免缓慢写入。ICEE 传输引擎能让分布式工作流实时完成数据分析。

## 四、HPC 应用案例

### 4.1 超新星搜索（Supernova Hunting）
PTF（Palomar Transient Factory）每 45 分钟扫描夜空，新图与旧图比对识别变化。**自动分类机器学习算法**让 SN 2011fe 在爆发后 11 小时被发现（手动检查根本来不及）。当前误标率 3.8%。

### 4.2 KSTAR 核聚变 Blob 检测
韩国 KSTAR 装置实验间隔 10-30 分钟，需要在这个窗口内完成跨国分布式分析。CIFT 用 ADIOS + ICEE 实现 ECEI（电子回旋发射成像）和 XGC 仿真数据的实时融合分析。新算法 MPI + 共享内存并行，**毫秒级完成每个时间步的 blob 检测**。

### 4.3 日内电力高峰预测
AMI（高级计量基础设施）产生海量电力消耗数据。CIFT 开发了 **LTAP**（log-temperature 分段线性 baseline）——一种 white-box 方法，比 black-box GTB（gradient tree boosting）更稳：

- GTB 用 lagged variables（前一天、前一周）做 feature，但用前一年的数据预测当年时**预测误差会累积**，几个月后预测就崩。
- LTAP 假设"每日电量是均温的分段线性函数"，加上"户用电 profile 在研究期内不变" → 自一致预测。
- 对照组的预测准确，能识别 active 与 passive 处理组的不同响应。

### 4.4 2010 Flash Crash
2010-05-06 14:45 EDT，道琼斯急跌 10%；Apple 一笔成交在 $100,000（交易所允许的最大价）。

CIFT 实现两个"早期警告"指标：
- **VPIN**（Easley, López de Prado, O'Hara 2011）
- **HHI**（市场分散度的 Herfindahl-Hirschman Index 变体）

用 C++ + MPI 实现，在 512 核上跑：
- 10 年 S&P500 数据，ASCII 文件单核 3.5 小时
- HDF5 文件单核 603.98 秒
- **HDF5 + 512 核：2.58 秒（加速 234 倍）**

### 4.5 VPIN 参数标定
67 个月的 100 个最活跃期货数据。原本 18 分钟跑完一个合约，HPC 实现 **1.5 秒**（**加速 720 倍**）——而且**只来自算法改进、不用并行**。

加速之后能做参数空间搜索。把 VPIN 的假阳性率从 20% 降到 7%。最优配置：
1. volume bar 用 trades 的中位价（不是收盘价）
2. 200 buckets/day
3. 30 bars/bucket
4. support window = 1 day，event duration = 0.1 day
5. bulk volume classification with Student t-dist $\nu=0.1$
6. VPIN CDF 阈值 = 0.99

不同期货类别可达更低假阳性率（< 1%）。利率、指数期货假阳性低；商品（能源、金属）更高。

### 4.6 Non-uniform FFT 揭示高频交易
天然气期货价格序列上跑 **NUFFT（Non-uniform Fast Fourier Transform）**：
- 频率 366（每天 1 次）amplitude 最高 → 2012 闰年自洽验证
- 频率 732（每天 2 次）、52（每周 1 次）也清晰
- **频率 527040（每分钟 1 次）异常强**——比邻近频率高 10+ 倍
- "每分钟 1 次"精确到秒 → **TWAP 算法的指纹**
- 高频成分在近年明显增强 → 算法交易扩张证据

## 五、总结与展望

主要论点：
1. **HPC 不是小众**——Higgs 粒子、引力波、行为经济学都靠它。
2. **NSCI（National Strategic Computing Initiative, 2015）和 HPC4Manufacturing** 已在推动 HPC 向商业迁移。
3. **CIFT 实证证明**：
   - HDF5 加速数据访问 **21 倍**
   - VPIN 计算加速 **720 倍**
   - 这种性能足以让 flash crash 早期警告"提前足够多时间"采取行动
4. **HPC 比云便宜 50% 到 7 倍**——尤其是对需要不断 ingest 数据的复杂分析任务。

## 六、本章给金融 ML 从业者的实操启示

1. **不要默认用云**——流式金融数据分析下，云的虚拟化代价显著，HPC 优势明确。
2. **数据存 HDF5**——单核加速 21 倍是不需要做其他改变就能拿到的红利。
3. **MPI 才是真正的进程间通信工具**——比 Python 多进程级别高一个量级。
4. **流式 in-situ 处理**避免大量数据落盘 → ADIOS / ICEE。
5. **算法改进 > 暴力并行**：VPIN 案例的 720× 大部分来自数据结构优化，不是机器数量。
6. **flash crash 等"反常事件"是 HPC 价值最高的场景**——预警时窗以分钟计。
7. **NUFFT 等信号处理工具**能从非均匀采样的金融时序里挖出算法交易指纹。
8. **白盒模型在 lag 累积场景下优于黑盒**（GTB vs LTAP 案例）——金融跨时序场景下值得借鉴。
9. **与国家实验室合作不再遥不可及**——CIFT 已有现成基础设施开放接入（http://crd.lbl.gov/cift/）。

## 七、关联阅读

- 上游：多进程编程基础 → [[chapter-20-multiprocessing]]
- 上游：量子计算与暴力搜索 → [[chapter-21-brute-force-quantum]]
- 应用：VPIN 与 PIN 完整理论 → [[chapter-19-microstructural-features]]
- 应用：SADF 等计算密集的特征 → [[chapter-17-structural-breaks]]
- 应用：分钟级 TWAP 识别 → [[chapter-19-microstructural-features]]
- 整体：作者第 1 章对国家实验室协作模式的赞许 → [[chapter-01-financial-ml-as-distinct-subject]]
- 参考：Easley, López de Prado, O'Hara (2011) "The microstructure of the flash crash"
- 参考：Wu et al. (2013) "A big data approach to analyzing market volatility"
- 参考：Holzman et al. (2017) HEPCloud 成本对比
- 参考：CIFT 项目主页 http://crd.lbl.gov/cift/
