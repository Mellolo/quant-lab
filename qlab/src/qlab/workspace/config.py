"""ExperimentConfig — 实验配置的代码化定义."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class ExperimentConfig:
    """一个实验的完整配置."""

    name: str
    dataset_id: str
    universe: str
    date_range: tuple[str, str]
    features: list[str | dict] = field(default_factory=list)
    labeling: dict[str, Any] = field(default_factory=dict)
    weights: dict[str, Any] = field(default_factory=dict)
    model: dict[str, Any] = field(default_factory=dict)
    cv: dict[str, Any] = field(default_factory=dict)
    evaluation: dict[str, Any] = field(default_factory=dict)
    notes: str = ""

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ExperimentConfig:
        return cls(
            name=d["name"],
            dataset_id=d["dataset_id"],
            universe=d["universe"],
            date_range=tuple(d["date_range"]),
            features=d.get("features", []),
            labeling=d.get("labeling", {}),
            weights=d.get("weights", {}),
            model=d.get("model", {}),
            cv=d.get("cv", {}),
            evaluation=d.get("evaluation", {}),
            notes=d.get("notes", ""),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "dataset_id": self.dataset_id,
            "universe": self.universe,
            "date_range": list(self.date_range),
            "features": self.features,
            "labeling": self.labeling,
            "weights": self.weights,
            "model": self.model,
            "cv": self.cv,
            "evaluation": self.evaluation,
            "notes": self.notes,
        }


def load_config(path: str | Path) -> ExperimentConfig:
    with open(path) as f:
        data = yaml.safe_load(f)
    return ExperimentConfig.from_dict(data)


def save_config(config: ExperimentConfig, path: str | Path) -> None:
    with open(path, "w") as f:
        yaml.safe_dump(config.to_dict(), f, sort_keys=False, allow_unicode=True)
