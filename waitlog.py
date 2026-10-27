"""
排队等待数据收集（以后用来训练"要等多久"的模型）
==============================================
每次刷新成功一个集群后调用 WaitLog.update()，在 waitlog/ 目录里维护：

- jobs.csv：每个 job 一行（集群 + job id），每次刷新整体重写。
  请求的资源、第一次看到它在排队时的集群状态（"当时的 cluster 状态"）、
  最终的开始时间和等待时间。没开始就结束/还在排的标 censored=1。
- qos_snapshots.csv：每次刷新追加：每个 QoS 全集群/我的 运行/排队数。
- partition_snapshots.csv：每次刷新追加：每个分区全集群的 运行/排队数、我的 运行/排队数，
  有积压数据的再加积压范围（× GPU 型号）。

快照表可以按时间和 jobs.csv 里的 eligible/submit 时间对上，拿到更精确的"提交时的状态"。
时间都是集群自己的本地时间字符串（YYYY-MM-DDTHH:MM:SS）；observed_at 是本机时间。
"""

import csv
import os
import re
import tarfile
import threading
from datetime import datetime, timedelta, timezone

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
    # 当时这个分区全集群的运行 / 排队数（所有 QoS）；当时预测模型给的排队时间（以后用来检验模型）
    "obs_part_all_running", "obs_part_all_pending", "obs_ml_q20_h", "obs_ml_q50_h", "obs_ml_q80_h",
]
QOS_COLUMNS = ["observed_at", "cluster", "qos", "all_running", "all_pending",
               "mine_running", "mine_pending", "max_jobs_pu", "max_submit_pu", "max_wall"]
PART_COLUMNS = ["backlog_at", "cluster", "partition", "cpus", "gpus", "cpu_min_h", "cpu_max_h",
                "gpu_type", "gpu_min_h", "gpu_max_h", "mine_running", "mine_pending",
                # 后来加的：每次刷新都写（本机时间），全集群在这个分区的运行 / 排队数（看不到就空）
                "observed_at", "all_running", "all_pending"]


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
        self.lock = threading.Lock()  # update() 在后台线程跑，history() 在网页请求里读
        os.makedirs(directory, exist_ok=True)
        self._load()

    def _load(self):
        try:
            with open(self.jobs_path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    self.rows[(row.get("cluster", ""), row.get("jobid", ""))] = row
        except OSError:
            pass

    # ---------------- 写文件 ----------------
    def _append(self, path, columns, rows):
        if not rows:
            return
        if os.path.exists(path):
            with open(path, newline="", encoding="utf-8") as f:
                header = next(csv.reader(f), None)
            if header and header != columns and set(header) <= set(columns):
                # 只是加了列：把旧数据搬到新表头下（新列留空）
                with open(path, newline="", encoding="utf-8") as f:
                    old = list(csv.DictReader(f))
                tmp = path + ".tmp"
                with open(tmp, "w", newline="", encoding="utf-8") as f:
                    w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
                    w.writeheader()
                    w.writerows(old)
                os.replace(tmp, path)
            elif header != columns:  # 列变了：旧文件改名保留，新开一个
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
        ov_parts = (result.get("overview") or {}).get("partitions", {})
        with self.lock:
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
                # 记"第一次看到它排队时"的状态；那时还在等 dependency 的，等它真正开始排队后再记一次
                # （等依赖时的集群状态和它什么时候开始没关系）
                if j["state"] in PENDING and is_live(j) and (
                        row.get("seen_pending") != "1"
                        or (waiting_dep(row.get("obs_reason")) and not waiting_dep(j.get("reason")))):
                    row["seen_pending"] = "1"
                    self._fill_obs(row, j, now, qos_rows.get(j.get("qos")), bl_parts, bl_at,
                                   load_private, ov_parts)
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
        # 分区快照：每次刷新每个分区一行（全集群 / 我的 运行排队数）；有积压数据的带上积压范围，
        # GPU 分区再每种卡一行
        rows = []
        for name in dict.fromkeys(list(ov_parts) + list(bl_parts)):
            o, p = ov_parts.get(name, {}), bl_parts.get(name)
            base = {"observed_at": now_s, "cluster": cluster, "partition": name, "gpu_type": "",
                    "all_running": "" if o.get("all_running") is None else o["all_running"],
                    "all_pending": "" if o.get("all_pending") is None else o["all_pending"],
                    "mine_running": o.get("mine_running", p["mine_running"] if p else ""),
                    "mine_pending": o.get("mine_pending", p["mine_pending"] if p else ""),
                    "cpus": ((o.get("cpus") or {}).get("total") or "") if not p else p["cpus"]}
            if p:
                base.update({"backlog_at": bl.get("fetched_at"), "gpus": p["gpus"],
                             "cpu_min_h": fmt(p["cpu_min"]), "cpu_max_h": fmt(p["cpu_max"])})
            rows.append(base)
            for g in (p or {}).get("gpu", []):
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
    def _fill_obs(row, j, now, q, bl_parts, bl_at, load_private, ov_parts):
        est = j.get("wait_est") or {}
        ml = (j.get("ml_est") or {}).get("wait") or ["", "", ""]
        pname = est.get("partition") or (j.get("ml_est") or {}).get("partition") \
            or (j.get("partition") or "").split(",")[0]
        part = bl_parts.get(pname, {})
        ov = ov_parts.get(pname, {})
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
            "obs_part_all_running": "" if ov.get("all_running") is None else ov["all_running"],
            "obs_part_all_pending": "" if ov.get("all_pending") is None else ov["all_pending"],
            "obs_ml_q20_h": fmt(ml[0]), "obs_ml_q50_h": fmt(ml[1]), "obs_ml_q80_h": fmt(ml[2]),
        })

    @staticmethod
    def _fill_outcome(row, j, now_s):
        state = j.get("state", "")
        row["state"] = state
        # 排队中的 job，squeue 的 start 是"预计"开始时间，不能当真实开始；
        # 排队时被取消的 job，有的 Slurm 版本会把 Start 设成取消时间，所以还要求分到过节点
        started = state in ACTIVE or (state not in PENDING and not is_live(j)
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

    # ---------------- 网页上的"排队历史" ----------------
    def history(self, days=None, cluster="", qos="", search="", limit=300, now=None):
        """按 集群 × QoS 统计已经开始的 job 等了多久，再列出最近的 job。
        days: 只看最近几天开始的（没开始的看 结束/可以排队/提交 时间；None = 全部）；qos = "集群|QoS"，只筛下面的 job 列表，
        统计表总是全部 QoS"""
        now = now or datetime.now()
        since = now - timedelta(days=days) if days else None
        needle = search.lower()
        with self.lock:
            rows = [dict(r) for r in self.rows.values()]
        picked = []
        for r in rows:
            # 按"最近一次有动静"的时间算：依赖链里的 job 常常很早提交、很晚才开始
            t = next((x for x in map(parse_ts, (r.get("start"), r.get("end"), r.get("eligible"),
                                                 r.get("submit"), r.get("first_seen_at"))) if x), None)
            if since and (not t or t < since):
                continue
            if cluster and r.get("cluster") != cluster:
                continue
            if needle and needle not in " ".join(r.get(k) or "" for k in (
                    "cluster", "jobid", "name", "qos", "partition", "run_partition", "nodelist",
                    "state", "gpu_type")).lower():
                continue
            picked.append((t or datetime.min, r))

        groups = {}
        for _, r in picked:
            g = groups.setdefault((r.get("cluster", ""), r.get("qos", "")), {"waits": [], "pending": 0, "no_start": 0})
            w = to_float(r.get("wait_from_eligible_h"))
            if r.get("censored") == "0" and w is not None:
                g["waits"].append(max(w, 0.0))
            elif r.get("state") in PENDING:
                g["pending"] += 1
            else:
                g["no_start"] += 1  # 排队时被取消 / 没观察到开始
        stats = []
        for (c, q), g in sorted(groups.items()):
            ws = sorted(g["waits"])
            stats.append({"cluster": c, "qos": q, "started": len(ws), "pending": g["pending"],
                          "no_start": g["no_start"],
                          "p25": quantile(ws, .25), "median": quantile(ws, .5),
                          "p75": quantile(ws, .75), "p90": quantile(ws, .9),
                          "max": ws[-1] if ws else None})

        q_cluster, _, q_name = qos.partition("|")
        jobs = [r for _, r in sorted(picked, key=lambda x: x[0], reverse=True)
                if not qos or (r.get("cluster") == q_cluster and (r.get("qos") or "") == q_name)]
        out = [{
            "cluster": r.get("cluster"), "jobid": r.get("jobid"), "name": r.get("name"),
            "qos": r.get("qos"), "partition": r.get("run_partition") or r.get("partition"),
            "cpus": r.get("cpus"), "gpus": r.get("gpus"), "gpu_type": r.get("gpu_type"),
            "nodelist": r.get("nodelist"), "state": r.get("state"),
            "submit": r.get("submit"), "eligible": r.get("eligible"),
            "start": r.get("start"), "end": r.get("end"),
            "wait_h": to_float(r.get("wait_from_eligible_h")),
            "wait_submit_h": to_float(r.get("wait_from_submit_h")),
            "censored": r.get("censored") != "0",
        } for r in jobs[:limit]]
        return {"stats": stats, "jobs": out, "total": len(jobs)}


def waiting_dep(reason):
    """squeue 的 Dependency / SGE 的 'Dependency (hqw)'"""
    return (reason or "").strip("()").startswith("Dependency")


def is_live(j):
    """这条记录来自当前队列（squeue / qstat），而不是历史记录（sacct / accounting）"""
    return j.get("live", j.get("source") == "squeue")


def to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def quantile(sorted_vals, q):
    if not sorted_vals:
        return None
    pos = (len(sorted_vals) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(sorted_vals) - 1)
    return round(sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (pos - lo), 3)


# ---------------- 大小上限 / 定期备份 ----------------
BACKUP_RE = re.compile(r"^waitlog-(\d{8}-\d{6})\.tar\.gz$")


def dir_size(path):
    """目录下所有文件的总字节数"""
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total


def list_backups(backup_dir):
    """[(备份时间, 路径), ...]，旧的在前"""
    out = []
    try:
        names = os.listdir(backup_dir)
    except OSError:
        return out
    for n in names:
        m = BACKUP_RE.match(n)
        if m:
            out.append((datetime.strptime(m.group(1), "%Y%m%d-%H%M%S"), os.path.join(backup_dir, n)))
    return sorted(out)


def latest_mtime(path):
    newest = 0.0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                newest = max(newest, os.path.getmtime(os.path.join(root, f)))
            except OSError:
                pass
    return newest


def backup_due(src, backup_dir, days, now=None):
    """最新的备份已经超过 days 天（或者还没有备份）就该备份了"""
    if not days or days <= 0 or not os.path.isdir(src):
        return False
    now = now or datetime.now()
    backups = list_backups(backup_dir)
    if not backups:
        return latest_mtime(src) > 0  # 空目录不备份
    last_at = backups[-1][0]
    if now - last_at < timedelta(days=days):
        return False
    # 上次备份之后什么都没改过（比如超过大小上限停写了），不用再打一份一模一样的
    return latest_mtime(src) > last_at.timestamp()


def make_backup(src, backup_dir, keep=10, now=None):
    """把 src 打包成 backup_dir/waitlog-YYYYmmdd-HHMMSS.tar.gz，只留最新的 keep 份"""
    now = now or datetime.now()
    os.makedirs(backup_dir, exist_ok=True)
    path = os.path.join(backup_dir, f"waitlog-{now:%Y%m%d-%H%M%S}.tar.gz")
    tmp = path + ".tmp"
    skip = os.path.abspath(backup_dir) + os.sep

    def keep_member(ti):
        return None if ti.name.endswith(".tmp") else ti

    try:
        with tarfile.open(tmp, "w:gz") as tar:
            for root, dirs, files in os.walk(src):
                # 备份目录要是放在 waitlog 里面，别把旧备份也打进去
                dirs[:] = [d for d in dirs if not (os.path.abspath(os.path.join(root, d)) + os.sep).startswith(skip)]
                for f in sorted(files):
                    full = os.path.join(root, f)
                    arc = os.path.join(os.path.basename(os.path.abspath(src)), os.path.relpath(full, src))
                    tar.add(full, arcname=arc, filter=keep_member)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise
    removed = []
    if keep and keep > 0:
        for _, old in list_backups(backup_dir)[:-keep]:
            try:
                os.remove(old)
                removed.append(old)
            except OSError:
                pass
    return path, removed
