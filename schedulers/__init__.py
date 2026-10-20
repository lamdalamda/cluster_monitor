"""各种调度系统。config.yaml 里每个集群用 `scheduler: slurm | sge` 选（默认 slurm）。

加一种新的（比如 PBS）：在这个目录里写一个 Scheduler 的子类（见 base.py 的说明），
再登记到下面的 SCHEDULERS。"""

from .base import ACTIVE, ALL_FEATURES, PENDING, Scheduler
from .sge import SGE
from .slurm import Slurm

SCHEDULERS = {cls.kind: cls for cls in (Slurm, SGE)}


def make(cluster, config):
    kind = str(cluster.get("scheduler") or config.get("scheduler") or "slurm").lower()
    if kind not in SCHEDULERS:
        raise ValueError(f"unknown scheduler '{kind}' for cluster {cluster.get('name')} "
                         f"(supported: {', '.join(SCHEDULERS)})")
    return SCHEDULERS[kind](cluster, config)


__all__ = ["ACTIVE", "ALL_FEATURES", "PENDING", "SCHEDULERS", "SGE", "Scheduler", "Slurm", "make"]
