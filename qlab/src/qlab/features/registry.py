"""特征注册表 — 全局唯一索引 + 版本管理.

提供装饰器风格的注册：

    @registry.register
    class Momentum5D(DailyFeature):
        meta = FeatureMeta(name='mom_5d', ...)
        def compute(self, ctx): ...
"""

from __future__ import annotations

from typing import TypeVar

from qlab.core.exceptions import FeatureRegistrationError
from qlab.features.base import Feature

F = TypeVar("F", bound=type)


class FeatureRegistry:
    """全局特征注册表."""

    def __init__(self) -> None:
        self._registry: dict[str, Feature] = {}
        self._classes: dict[str, type] = {}

    def register(self, cls: F | None = None, *, instance: Feature | None = None) -> F:
        """注册一个特征类或实例.

        Note:
            传 ``instance`` 时**直接注册该实例**, 不能用 ``type(instance)()``
            重建 —— 那会丢掉构造参数, 使 ``Momentum(5)/Momentum(10)/
            Momentum(20)`` 全变成默认参数的同一个因子(且同名幂等忽略),
            参数化因子静默丢失。
        """

        def _do_register(inst: Feature, target_cls: type | None = None) -> None:
            name = inst.meta.name
            if name in self._registry:
                existing_version = self._registry[name].meta.version
                if existing_version != inst.meta.version:
                    raise FeatureRegistrationError(
                        f"特征 '{name}' 已存在版本 {existing_version}，"
                        f"新注册版本 {inst.meta.version} 冲突。请改名或调整版本。"
                    )
                return  # 同名同版本: 幂等
            self._registry[name] = inst
            if target_cls is not None:
                self._classes[name] = target_cls

        if instance is not None:
            _do_register(instance, type(instance))
            return type(instance)  # type: ignore[return-value]
        if cls is not None:
            _do_register(cls(), cls)
            return cls

        def _decorator(target_cls: F) -> F:
            _do_register(target_cls(), target_cls)
            return target_cls

        return _decorator  # type: ignore[return-value]

    def get(self, name: str) -> Feature:
        """按注册名取因子.

        Raises:
            KeyError: 未注册。注册表为空时额外提示导入遗漏 ——
                因子靠**导入副作用**注册(``library/*.py`` 底部调 register),
                忘记 import 时表是空的, 光报"已注册: []"很难定位。
        """
        if name not in self._registry:
            if not self._registry:
                raise KeyError(
                    f"特征 '{name}' 未注册 —— 注册表是**空**的。\n"
                    "  因子靠导入副作用注册, 请先导入因子库:\n"
                    "      import qlab.features.library\n"
                    "  (或 from qlab.features.library import Momentum, ... )\n"
                    "  自定义因子需先 registry.register(instance=YourFeature(...))。"
                )
            raise KeyError(
                f"特征 '{name}' 未注册。已注册: {sorted(self._registry.keys())}"
            )
        return self._registry[name]

    def has(self, name: str) -> bool:
        return name in self._registry

    def all_names(self) -> list[str]:
        return sorted(self._registry.keys())

    def all_features(self) -> list[Feature]:
        return [self._registry[n] for n in self.all_names()]

    def clear(self) -> None:
        self._registry.clear()
        self._classes.clear()

    def __len__(self) -> int:
        return len(self._registry)

    def __repr__(self) -> str:
        return f"<FeatureRegistry n={len(self)}>"


# 全局单例
registry = FeatureRegistry()
