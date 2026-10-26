"""
调度系统的抽象基类
==================
Backend（app.py）只管 SSH、缓存、合并结果、给前端的数据；每种调度系统
（Slurm / SGE / 以后的 PBS）要跑什么命令、怎么解析输出，都在 Scheduler 的子类里。

统一用 Slurm 的词汇，这样前端、waitlog、backlog 都不用管是哪种调度系统：
- 状态：RUNNING / PENDING / COMPLETED / FAILED / CANCELLED / TIMEOUT ...
- "qos"：QoS 面板按它分组。Slurm 是 QoS，SGE 是 project（Free / Gold ...）
- "partition"：Slurm 的分区；SGE 用并行环境（mpi-w12a），没有就用队列名
- 时间：集群本地时间字符串 YYYY-MM-DDTHH:MM:SS

功能（FEATURES）：子类声明自己能做到什么，集群做不到的在 config.yaml 里用
`disable: [...]` 关掉（比如 bluebear 看不到别人的 job → disable: [cluster_load]；
不写也行，Backend 发现"全集群"数字和你自己的一样时会自动当成看不到）。
- history       已经结束的 job（sacct / SGE accounting 文件）
- cluster_load  所有用户在每个 QoS 上的 运行/排队 数
- qos_info      能用哪些 QoS 和它们的限额（sacctmgr）
- backlog       排队积压估计（backlog.py，要看到全集群的 job 和分区）
- est_start     调度器自己估计的开始时间
- partition_info 每个分区的节点 / CPU 用量和时限（sinfo）
"""

import re
from abc import ABC, abstractmethod

ACTIVE = {"RUNNING", "COMPLETING", "CONFIGURING"}
PENDING = {"PENDING", "SUSPENDED", "REQUEUED", "REQUEUE_HOLD", "RESV_DEL_HOLD"}

ALL_FEATURES = {"history", "cluster_load", "qos_info", "backlog", "est_start", "partition_info"}


class Scheduler(ABC):
    kind = ""            # config.yaml 里 scheduler: 的值
    qos_label = "QoS"    # 前端 QoS 面板的列名
    PARTITION_LOAD = False  # parse_partition_load 能给出全集群按分区的数字
    FEATURES = frozenset()

    def __init__(self, cluster, config):
        self.cluster = cluster
        self.config = config
        disabled = set(cluster.get("disable") or [])
        # 旧的写法：backlog: false / show_cluster_load: false（全局或单个集群）
        if not cluster.get("backlog", config.get("backlog", True)):
            disabled.add("backlog")
        if not cluster.get("show_cluster_load", config.get("show_cluster_load", True)):
            disabled.add("cluster_load")
        self.features = set(self.FEATURES) - disabled

    def has(self, feature):
        return feature in self.features

    # ---------------- 子类必须实现 ----------------
    @abstractmethod
    def steps(self, want):
        """这次 SSH 要跑的命令 [(SECTION, cmd), ...]，SECTION 全大写。
        want: {"history_hours": int, "load": bool, "qos_info": bool, "backlog": bool,
               "partition_info": bool}
        （load / qos_info / backlog / partition_info 只在 has() 为真时才会是 True）"""

    @abstractmethod
    def required_sections(self):
        """这些段全都失败 = 这次刷新失败（比如 squeue 和 sacct 都跑不了）"""

    @abstractmethod
    def parse_jobs(self, sections, prev_jobs):
        """返回 {jobid: job dict}：当前在队列里的 + 最近结束的。
        prev_jobs：上一次成功的结果，用来补上这次拿不到的信息（比如 SGE 的历史记录里没有工作目录）。

        job dict 的字段（没有的留空字符串）：
          id cluster name partition qos state time_used time_limit nnodes nodelist reason
          submit eligible start end cpus tres req_tres workdir priority
          dependency_raw deps=[{"type","id","status"}] source live
        live=True 表示这条来自当前队列（squeue/qstat），False 表示来自历史记录；
        排队中的 job，start 是调度器自己估计的开始时间（没有就空）。"""

    # ---------------- 可选：有对应功能才需要 ----------------
    def parse_load(self, sections):
        """{qos: {"running": n, "pending": n}}"""
        return {}

    def parse_partition_load(self, sections):
        """{分区: {"running": n, "pending": n}}（排队中的 job 可以同时排在好几个分区，每个都算）"""
        return {}

    def parse_partitions(self, sections):
        """[{"name", "default", "avail", "time_limit", "nodes": {alloc, idle, other, total},
            "cpus": {alloc, idle, other, total}}]；拿不到返回 None"""
        return None

    def optional_sections(self):
        """失败了也不在页面上报错的段（比如有的集群不让用 sinfo，卡片里会说明）"""
        return set()

    def parse_qos_info(self, sections):
        """{"assoc": [能用的 QoS], "limits": {qos: {max_jobs_pu, max_submit_pu, max_wall, max_tres_pu}}}"""
        return {"assoc": [], "limits": {}}

    def analyze_backlog(self, sections):
        """返回 (给前端的 result, 给 estimate_job 用的 model)"""
        raise NotImplementedError

    def estimate_job(self, job, model):
        return None


# ---------------- 子类共用的小工具 ----------------
def clean_time(v):
    return "" if v in ("N/A", "Unknown", "None", "NONE", "", None) else v


def fmt_duration(seconds):
    """秒 -> Slurm 风格的 [D-]HH:MM:SS"""
    try:
        s = int(float(seconds))
    except (TypeError, ValueError):
        return ""
    if s < 0:
        return ""
    d, s = divmod(s, 86400)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    return f"{d}-{h:02d}:{m:02d}:{s:02d}" if d else f"{h:02d}:{m:02d}:{s:02d}"


def parse_lines(lines, fields, sep="|"):
    """'a|b|c' 一行一条 -> [dict]，字段数不对的行跳过（登录脚本打印的杂七杂八）"""
    out = []
    for line in lines:
        parts = line.split(sep)
        if len(parts) == len(fields):
            out.append(dict(zip(fields, parts)))
    return out


def sort_jobid(jid):
    m = re.match(r"(\d+)", jid)
    return int(m.group(1)) if m else 0
