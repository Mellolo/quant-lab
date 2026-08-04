"""数据源实现.

接入 tushare/akshare/wind 等具体源时，在此目录新增对应模块。

- :class:`FakeDataSource`：合成数据（测试/开发用，无外部依赖）
- :class:`JQDataSource`：聚宽 JoinQuant（需 ``pip install -e ./jq``）

``JQDataSource`` 延迟导入：未安装 jq 连接器时仍可正常使用 ``FakeDataSource``。
"""

from qlab.data.sources.fake import FakeDataSource
from qlab.data.sources.jq_source import JQDataSource, to_jq_code, to_qlab_symbol

__all__ = ["FakeDataSource", "JQDataSource", "to_jq_code", "to_qlab_symbol"]
