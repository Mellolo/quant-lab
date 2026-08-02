# 第 20 章：多进程与向量化（Multiprocessing and Vectorization）

> 全书反复出现的 `mpPandasObj` 在这一章揭晓。ML 计算密集，必须高效利用所有 CPU / 集群。本章给出从向量化到三层并行（向量化 + 多进程 + HPC 集群）的工程方案。

## 一、动机与多线程的局限

Python 因为 **GIL（Global Interpreter Lock）**，多线程一次只能让一个线程获得写权限——实际上不能并行。Python 真正的并行靠**多进程**。

- 多线程优势：共享内存（但有 race 风险）
- 多进程：不共享内存（无 race），但**进程间传对象更难**

## 二、向量化（Vectorization）

最简单的并行——对整个数组一次性运算。

### 反例（嵌套 for 循环的笛卡尔积）：
```python
dict0 = {'a': ['1', '2'], 'b': ['+', '*'], 'c': ['!', '@']}
for a in dict0['a']:
    for b in dict0['b']:
        for c in dict0['c']:
            print({'a': a, 'b': b, 'c': c})
```
100 维就要 100 层 for。

### 向量化版本：
```python
from itertools import izip, product
jobs = (dict(izip(dict0, i)) for i in product(*dict0.values()))
for i in jobs: print(i)
```

四个优势：
1. 慢的嵌套 for 替换为快的 iterator
2. 自动推断维度
3. 100 维不用改代码
4. 底层可走 C/C++

## 三、单线程 vs 多线程 vs 多进程

现代电脑：多 socket × 多 core × 多 thread。

### 3.1 单线程示例（10000 条高斯路径找最早触碰双屏障）：

```python
def main0():
    r = np.random.normal(0, .01, size=(1000, 10000))
    t = barrierTouch(r)

def barrierTouch(r, width=.5):
    t, p = {}, np.log((1+r).cumprod(axis=0))
    for j in range(r.shape[1]):
        for i in range(r.shape[0]):
            if p[i,j] >= width or p[i,j] <= -width:
                t[j] = i; continue
    return t
```

### 3.2 多进程版本（24 cores）：
```python
def main1():
    r, numThreads = np.random.normal(0, .01, size=(1000, 10000)), 24
    parts = np.linspace(0, r.shape[0], min(numThreads, r.shape[0])+1)
    parts = np.ceil(parts).astype(int)
    jobs = [r[:, parts[i-1]:parts[i]] for i in range(1, len(parts))]
    pool = mp.Pool(processes=numThreads)
    outputs = pool.imap_unordered(barrierTouch, jobs)
    out = list(outputs)
    pool.close(); pool.join()
```

5000 CPU 集群上 → 约单线程 1/5000 时间。

### 3.3 三层并行
- 向量化（指令级）
- 多进程（节点内多核）
- HPC 集群（多节点）

## 四、Atoms（原子任务）与 Molecules（分子）

- **Atom**：不可分割的最小任务
- **Molecule**：被分到一个核上**串行执行**的原子集合
- **并行发生在 molecule 层级**

### 4.1 Linear Partitions（线性分割）

最简单：等分原子数。$N+1$ 个索引切出 $N$ 个分子。

```python
def linParts(numAtoms, numThreads):
    parts = np.linspace(0, numAtoms, min(numThreads, numAtoms)+1)
    return np.ceil(parts).astype(int)
```

### 4.2 Two-Nested Loops Partitions（嵌套循环分割）★

很多场景是双层循环（SADF、triple-barrier、错位序列协方差）。每个外层 $i$ 内层范围 $1, \ldots, i$ → 三角形结构。

线性等分会让靠后的 molecule 任务量大很多 → 处理时间被最重 molecule 决定。

更优分法：让每个 molecule 都包含 **$\frac{1}{2M}N(N+1)$** 个任务。第 $m$ 个 molecule 的最后一行：
$$
r_m = \frac{-1 + \sqrt{1 + 4(r_{m-1}^2 + r_{m-1} + N(N+1)M^{-1})}}{2}
$$

```python
def nestedParts(numAtoms, numThreads, upperTriang=False):
    parts, numThreads_ = [0], min(numThreads, numAtoms)
    for num in range(numThreads_):
        part = 1 + 4*(parts[-1]**2 + parts[-1] + numAtoms*(numAtoms+1.)/numThreads_)
        part = (-1 + part**.5) / 2.
        parts.append(part)
    parts = np.round(parts).astype(int)
    if upperTriang:
        parts = np.cumsum(np.diff(parts)[::-1])
        parts = np.append(np.array([0]), parts)
    return parts
```

`upperTriang=True` 用于上三角嵌套（内层 $j = i, \ldots, N$）。本质是 bin packing 的特例。

## 五、多进程引擎（Multiprocessing Engines）

不要为每个函数单独写并行 wrapper——开发一个**通用引擎**。

### 5.1 mpPandasObj 的 6 个参数

```python
def mpPandasObj(func, pdObj, numThreads=24, mpBatches=1, linMols=True, **kargs):
```

- `func`：要并行执行的回调
- `pdObj`：(传给 func 的分子参数名, 原子列表)
- `numThreads`：核心数
- `mpBatches`：**每核任务批数**——大于 1 时分子数 > 核心数，避免重 molecule 拖慢全队
- `linMols`：线性分割还是嵌套分割
- `kargs`：func 的其他关键字参数

### 5.2 mpBatches 的妙用

假设 10 个 molecule 中第 1 个耗时 2x。如果 mpBatches=1 → 9 个核在等。
设 mpBatches=10 → 切成 100 个 molecule，每个核拿到等量负载，总时间减半。

### 5.3 工作流程

```python
def mpPandasObj(func, pdObj, numThreads=24, mpBatches=1, linMols=True, **kargs):
    if linMols:
        parts = linParts(len(pdObj[1]), numThreads*mpBatches)
    else:
        parts = nestedParts(len(pdObj[1]), numThreads*mpBatches)
    jobs = []
    for i in range(1, len(parts)):
        job = {pdObj[0]: pdObj[1][parts[i-1]:parts[i]], 'func': func}
        job.update(kargs)
        jobs.append(job)
    if numThreads == 1:
        out = processJobs_(jobs)  # 调试用
    else:
        out = processJobs(jobs, numThreads=numThreads)
    if isinstance(out[0], pd.DataFrame):
        df0 = pd.DataFrame()
    elif isinstance(out[0], pd.Series):
        df0 = pd.Series()
    else:
        return out
    for i in out: df0 = df0.append(i)
    return df0.sort_index()
```

### 5.4 单线程调试模式

`numThreads=1` 走 `processJobs_`：
```python
def processJobs_(jobs):
    out = []
    for job in jobs:
        out.append(expandCall(job))
    return out
```

理由：**多进程 bug 难抓**（"Heisenbug"——多进程改变行为）。开发时单线程，OK 后再开多核。

### 5.5 异步调用

```python
def processJobs(jobs, task=None, numThreads=24):
    if task is None: task = jobs[0]['func'].__name__
    pool = mp.Pool(processes=numThreads)
    outputs, out, time0 = pool.imap_unordered(expandCall, jobs), [], time.time()
    for i, out_ in enumerate(outputs, 1):
        out.append(out_)
        reportProgress(i, len(jobs), time0, task)
    pool.close(); pool.join()  # 防内存泄漏
    return out
```

### 5.6 expandCall：把字典变成调用

```python
def expandCall(kargs):
    func = kargs['func']
    del kargs['func']
    return func(**kargs)
```

引擎的核心 trick：**把每个 job dict 中的 `func` 拿出来当回调，其他作为关键字参数传入**。

### 5.7 Pickle bug 修补

绑定方法不可 pickle。在引擎顶部加：
```python
def _pickle_method(method):
    func_name = method.im_func.__name__
    obj = method.im_self
    cls = method.im_class
    return _unpickle_method, (func_name, obj, cls)

def _unpickle_method(func_name, obj, cls):
    for cls in cls.mro():
        try: func = cls.__dict__[func_name]; break
        except KeyError: pass
    return func.__get__(obj, cls)

import copy_reg, types, multiprocessing as mp
copy_reg.pickle(types.MethodType, _pickle_method, _unpickle_method)
```

### 5.8 Output Reduction（输出聚合）★

如果 24 个 molecule 各返回一个大 DataFrame，先存 list 再合并会爆内存。**边返回边聚合**：

```python
def processJobsRedux(jobs, task=None, numThreads=24,
                     redux=None, reduxArgs={}, reduxInPlace=False):
    pool = mp.Pool(processes=numThreads)
    imap, out, time0 = pool.imap_unordered(expandCall, jobs), None, time.time()
    for i, out_ in enumerate(imap, 1):
        if out is None:
            if redux is None: out, redux, reduxInPlace = [out_], list.append, True
            else: out = copy.deepcopy(out_)
        else:
            if reduxInPlace: redux(out, out_, **reduxArgs)
            else: out = redux(out, out_, **reduxArgs)
        reportProgress(i, len(jobs), time0, task)
    pool.close(); pool.join()
    if isinstance(out, (pd.Series, pd.DataFrame)): out = out.sort_index()
    return out
```

- `redux=pd.DataFrame.add`：相加
- `redux=pd.DataFrame.join, reduxArgs={'how':'outer'}`：外联
- `redux=list.append` 或 `dict.update`：必须 `reduxInPlace=True`

## 六、综合应用例：稀疏 PCA 矩阵乘法

第 8 章 PCA：$P = Z\tilde W$。当 $Z$ 太大放不下内存时：

$$
P = Z\tilde W = \sum_{b=1}^{B} Z_b \tilde W_b
$$

把列分 $B$ 块，每块只装一列子集到 $Z_b$，并行累加。

```python
pcs = mpJobList(getPCs, ('molecules', fileNames), numThreads=24, mpBatches=1,
                path=path, eVec=eVec, redux=pd.DataFrame.add)

def getPCs(path, molecules, eVec):
    pcs = None
    for i in molecules:
        df0 = pd.read_csv(path+i, index_col=0, parse_dates=True)
        if pcs is None:
            pcs = np.dot(df0.values, eVec.loc[df0.columns].values)
        else:
            pcs += np.dot(df0.values, eVec.loc[df0.columns].values)
    return pd.DataFrame(pcs, index=df0.index, columns=eVec.columns)
```

两个优势：
1. **顺序加载** $Z_b$ → RAM 不爆
2. **并行执行 molecule** → 计算快

**很多 ML 问题没有并行根本算不动**——不是为了快，是为了**能算**。

## 七、本章实操要点

1. **能向量化就向量化**：itertools + numpy/pandas，避免显式 for。
2. **GIL 的存在让 Python 多线程几乎没用** → 用 multiprocessing。
3. **三层并行**：vectorization × 多进程 × HPC cluster。
4. **嵌套循环必须用 `nestedParts`**——线性分割对三角任务结构非常低效。
5. **`mpBatches > 1`** 让负载更均匀（heavy molecule 不拖后腿）。
6. **永远先 `numThreads=1` 调试 bug，再加多核**。
7. **输出大的任务必用 redux 聚合**——别 list 再合并，OOM 警告。
8. **加 pickle 补丁**，否则绑定方法不能传给子进程。
9. **大数据集 = 必须并行 + 流式聚合**。

## 八、关联阅读

- 全书用到 mpPandasObj 的地方：
  - [[chapter-03-labeling]]（triple-barrier）
  - [[chapter-04-sample-weights]]（uniqueness 计算）
  - [[chapter-08-feature-importance]]（SFI）
  - [[chapter-13-synthetic-backtesting]]（Monte Carlo OTR）
  - [[chapter-17-structural-breaks]]（SADF）
- 下游：穷举搜索 / 量子 → [[chapter-21-brute-force-quantum]]
- 国家实验室级 HPC → [[chapter-22-hpc-forecasting]]
- 参考：Ascher et al. (2005) Python Cookbook（第 7.5 节讲 pickle）
- 参考：Gorelick & Ozsvald (2008) High Performance Python
