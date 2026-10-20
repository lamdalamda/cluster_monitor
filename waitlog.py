"""
排队等待数据收集（以后用来训练"要等多久"的模型）
==============================================
每次刷新成功一个集群后调用 WaitLog.update()，在 waitlog/ 目录里维护：

- jobs.csv：每个 job 一行（集群 + job id），每次刷新整体重写。
  请求的资源、第一次看到它在排队时的集群状态（"当时的 cluster 状态"）、
  最终的开始时间和等待时间。没开始就结束/还在排的标 censored=1。
- qos_snapshots.csv：每次刷新追加：每个 QoS 全集群/我的 运行/排队数。
- partition_snapshots.csv：积压数据更新时追加：每个分区（× GPU 型号）的积压范围。

快照表可以按时间和 jobs.csv 里的 eligible/submit 时间对上，拿到更精确的"提交时的状态"。
时间都是集群自己的本地时间字符串（YYYY-MM-DDTHH:MM:SS）；observed_at 是本机时间。
"""

import csv
import os
import re
from datetime import datetime, timezone

import backlog

ACTIVE = {"RUNNING", "COMPLETING", "CONFIGURING"}
PENDING = {"PENDING", "SUSPENDED", "REQUEUED", "REQUEUE_HOLD", "RESV_DEL_HOLD"}

JOB_COLUMNS = [
    # 身份
    "cluster", "jobid", "name",
    # 请求的资源
    "partition", "qos", "nodes", "cpus", "gpus", "gpu_type", "mem", "time_limit", "time_limit_h",
    "dependency",
    # 时间线（集群本地时间）
    "submit", "eligible", "start", "end", "state", "run_partition", "nodelist",
    # 结果：等待时长（小时）。censored=1 表示没观察到开始（还在排 / 排队时被取消），
    # 这时 wait_* 是"至少等了这么久"
    "wait_from_submit_h", "wait_from_eligible_h", "censored",
    # 我们什么时候看到它的（本机时间）
    "first_seen_at", "last_seen_at", "seen_pending",
    # 第一次看到它在排队时的状态（seen_pending=0 时为空）
    "obs_at", "obs_lag_h", "obs_reason", "obs_priority", "obs_slurm_est_start",
    "obs_slurm_est_wait_h",
    "obs_qos_all_running", "obs_qos_all_pending", "obs_qos_mine_running", "obs_qos_mine_pending",
    "obs_bl_partition", "obs_bl_cpu_min_h", "obs_bl_cpu_max_h", "obs_bl_gpu_min_h",
    "obs_bl_gpu_max_h", "obs_bl_bottleneck", "obs_bl_age_min",
]
QOS_COLUMNS = ["observed_at", "cluster", "qos", "all_running", "all_pending",
               "mine_running", "mine_pending", "max_jobs_pu", "max_submit_pu", "max_wall"]
PART_COLUMNS = ["backlog_at", "cluster", "partition", "cpus", "gpus", "cpu_min_h", "cpu_max_h",
                "gpu_type", "gpu_min_h", "gpu_max_h", "mine_running", "mine_pending"]


def parse_ts(s):
    try:
        return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
    except (TypeError, ValueError):
        return None


def hours_between(a, b):
    ta, tb = parse_ts(a), parse_ts(b)
    if not ta or not tb:
        return ""
    return round((tb - ta).total_seconds() / 3600, 4)


def parse_req_tres(tres):
    """sacct ReqTRES 'billing=10,cpu=10,gres/gpu=1,mem=40G,node=1' -> dict"""
    out = {}
    for seg in (tres or "").split(","):
        k, _, v = seg.partition("=")
        if k == "cpu":
            out["cpus"] = v
        elif k == "mem":
            out["mem"] = v
        elif k == "node":
            out["nodes"] = v
        elif k.startswith("gres/gpu"):
            typ = k.split(":", 1)[1] if ":" in k else ""
            if typ or "gpus" not in out:  # 带型号的优先
                out["gpus"], out["gpu_type"] = v, typ
    return out


def fmt(v, nd=3):
    if v is None or v == "":
        return ""
    if isinstance(v, float):
        return round(v, nd)
    return v


class WaitLog:
    def __init__(self, directory):
        self.dir = directory
        self.jobs_path = os.path.join(directory, "jobs.csv")
        self.qos_path = os.path.join(directory, "qos_snapshots.csv")
        self.part_path = os.path.join(directory, "partition_snapshots.csv")
        self.rows = {}  # (cluster, jobid) -> row dict
        self.last_backlog_at = {}  # cluster -> 上次写过的积压数据时间，避免重复写
        os.makedirs(directory, exist_ok=True)
        self._load()

    def _load(self):
        try:
            with open(self.jobs_path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    self.rows[(row.get("cluster", ""), row.get("jobid", ""))] = row
        except OSError:
            pass
        try:
            with open(self.part_path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    self.last_backlog_at[row.get("cluster", "")] = row.get("backlog_at", "")
        except OSError:
            pass

    # ---------------- 写文件 ----------------
    def _append(self, path, columns, rows):
        if not rows:
            return
        if os.path.exists(path):
            with open(path, newline="", encoding="utf-8") as f:
                header = next(csv.reader(f), None)
            if header != columns:  # 列变了：旧文件改名保留，新开一个
                os.replace(path, f"{path}.{datetime.now():%Y%m%d%H%M%S}.old")
        new = not os.path.exists(path)
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
            if new:
                w.writeheader()
            w.writerows(rows)

    def _write_jobs(self):
        tmp = self.jobs_path + ".tmp"
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=JOB_COLUMNS, extrasaction="ignore")
            w.writeheader()
            for key in sorted(self.rows, key=lambda k: (k[0], int(re.match(r"\d*", k[1]).group() or 0), k[1])):
                w.writerow(self.rows[key])
        os.replace(tmp, self.jobs_path)

    # ---------------- 更新 ----------------
    def update(self, result, now=None):
        """result: app.Backend.poll_cluster() 成功时的返回值"""
        now = now or datetime.now()
        now_s = now.strftime("%Y-%m-%dT%H:%M:%S")
        cluster = result["name"]
        qos_rows = {q["name"]: q for q in result.get("qos", [])}
        bl = result.get("backlog") or {}
        bl_parts = {p["name"]: p for p in bl.get("partitions", [])} if bl.get("ok") else {}
        try:  # 只用来算"积压数据多旧"
            bl_at = datetime.fromisoformat(bl["fetched_at"]) if bl.get("fetched_at") else None
        except ValueError:
            bl_at = None
        load_private = result.get("load_private")

        for jid, j in result.get("jobs", {}).items():
            if "[" in jid:  # 还没展开的 array 排队记录（123_[1-10]），等各个 task 出现再记
                continue
            key = (cluster, jid)
            row = self.rows.get(key)
            if row is None:
                row = {"cluster": cluster, "jobid": jid, "first_seen_at": now_s, "seen_pending": "0"}
                self.rows[key] = row
            self._fill_static(row, j)
            row["last_seen_at"] = now_s
            if j["state"] in PENDING and row.get("seen_pending") != "1" and j.get("source") == "squeue":
                row["seen_pending"] = "1"
                self._fill_obs(row, j, now, qos_rows.get(j.get("qos")), bl_parts, bl_at,
                               load_private)
            self._fill_outcome(row, j, now_s)

        self._write_jobs()
        self._append(self.qos_path, QOS_COLUMNS, [{
            "observed_at": now_s, "cluster": cluster, "qos": q["name"],
            "all_running": "" if q.get("all_running") is None else q["all_running"],
            "all_pending": "" if q.get("all_pending") is None else q["all_pending"],
            "mine_running": q["mine_running"], "mine_pending": q["mine_pending"],
            "max_jobs_pu": q.get("max_jobs_pu", ""), "max_submit_pu": q.get("max_submit_pu", ""),
            "max_wall": q.get("max_wall", ""),
        } for q in result.get("qos", [])])
        if bl_parts and bl.get("fetched_at") != self.last_backlog_at.get(cluster):
            self.last_backlog_at[cluster] = bl.get("fetched_at")
            rows = []
            for p in bl_parts.values():
                base = {"backlog_at": bl.get("fetched_at"), "cluster": cluster,
                        "partition": p["name"], "cpus": p["cpus"], "gpus": p["gpus"],
                        "cpu_min_h": fmt(p["cpu_min"]), "cpu_max_h": fmt(p["cpu_max"]),
                        "mine_running": p["mine_running"], "mine_pending": p["mine_pending"]}
                rows.append({**base, "gpu_type": ""})
                for g in p["gpu"]:
                    rows.append({**base, "gpu_type": g["type"], "gpu_min_h": fmt(g["min"]),
                                 "gpu_max_h": fmt(g["max"])})
            self._append(self.part_path, PART_COLUMNS, rows)

    @staticmethod
    def _fill_static(row, j):
        """请求的资源：有就填，不拿空值覆盖已有的"""
        req = parse_req_tres(j.get("req_tres", ""))
        gtype, gcount = backlog.parse_gpu(j.get("tres", ""))
        vals = {
            "name": j.get("name"), "partition": j.get("partition"), "qos": j.get("qos"),
            "nodes": j.get("nnodes") or req.get("nodes"),
            "cpus": j.get("cpus") or req.get("cpus"),
            "gpus": str(gcount) if gcount else req.get("gpus", ""),
            "gpu_type": gtype or req.get("gpu_type", ""),
            "mem": req.get("mem"),
            "time_limit": j.get("time_limit"),
            "time_limit_h": fmt(backlog.parse_duration(j["time_limit"])) if j.get("time_limit") else "",
            "dependency": j.get("dependency_raw"),
            "submit": j.get("submit"), "eligible": j.get("eligible"),
        }
        for k, v in vals.items():
            if v not in (None, "") and not row.get(k):
                row[k] = v
        if not row.get("gpus"):
            row["gpus"] = "0"

    @staticmethod
    def _fill_obs(row, j, now, q, bl_parts, bl_at, load_private):
        est = j.get("wait_est") or {}
        part = bl_parts.get(est.get("partition") or (j.get("partition") or "").split(",")[0], {})
        gpu = next((g for g in part.get("gpu", [])
                    if est and g["type"] in est.get("bottleneck", {}).get("label", "")), None)
        ref = j.get("eligible") or j.get("submit")
        row.update({
            "obs_at": now.strftime("%Y-%m-%dT%H:%M:%S"),
            "obs_lag_h": hours_between(ref, now.strftime("%Y-%m-%dT%H:%M:%S")),
            "obs_reason": j.get("reason", ""),
            "obs_priority": j.get("priority", ""),
            "obs_slurm_est_start": j.get("start", ""),
            "obs_slurm_est_wait_h": hours_between(ref, j.get("start")) if j.get("start") else "",
            "obs_qos_all_running": "" if not q or load_private else q.get("all_running", ""),
            "obs_qos_all_pending": "" if not q or load_private else q.get("all_pending", ""),
            "obs_qos_mine_running": q.get("mine_running", "") if q else "",
            "obs_qos_mine_pending": q.get("mine_pending", "") if q else "",
            "obs_bl_partition": part.get("name", ""),
            "obs_bl_cpu_min_h": fmt(part.get("cpu_min")) if part else "",
            "obs_bl_cpu_max_h": fmt(part.get("cpu_max")) if part else "",
            "obs_bl_gpu_min_h": fmt(gpu["min"]) if gpu else "",
            "obs_bl_gpu_max_h": fmt(gpu["max"]) if gpu else "",
            "obs_bl_bottleneck": est.get("bottleneck", {}).get("label", ""),
            "obs_bl_age_min": round((datetime.now(timezone.utc) - bl_at).total_seconds() / 60, 1)
                              if bl_at and part else "",
        })

    @staticmethod
    def _fill_outcome(row, j, now_s):
        state = j.get("state", "")
        row["state"] = state
        # 排队中的 job，squeue 的 start 是"预计"开始时间，不能当真实开始；
        # 排队时被取消的 job，有的 Slurm 版本会把 Start 设成取消时间，所以还要求分到过节点
        started = state in ACTIVE or (state not in PENDING and j.get("source") == "sacct"
                                      and parse_ts(j.get("start")) and j.get("nodelist"))
        if started and j.get("start"):
            row["start"] = j["start"]
            row["run_partition"] = j.get("partition", "")
            row["nodelist"] = j.get("nodelist", "")
        if j.get("end"):
            row["end"] = j["end"]
        if row.get("start"):
            row["censored"] = "0"
            end_ref = row["start"]
        else:
            row["censored"] = "1"
            # 没开始：已经结束（排队时被取消）就算到结束；还在排就算到现在（本机时间）
            end_ref = row.get("end") or now_s
        row["wait_from_submit_h"] = hours_between(row.get("submit"), end_ref)
        row["wait_from_eligible_h"] = hours_between(row.get("eligible") or row.get("submit"),
                                                    end_ref)
