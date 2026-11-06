"""Slurm：squeue / sacct / sacctmgr / sinfo"""

import math
import re
from bisect import bisect_left

import backlog

from .base import ACTIVE, PENDING, Scheduler, clean_time, parse_lines, sort_jobid

SQUEUE_FIELDS = [
    "jobid",
    "partition",
    "name",
    "user",
    "state",
    "time",
    "timelimit",
    "nnodes",
    "nodelist_reason",
    "qos",
    "submit",
    "start",
    "cpus",
    "tres",
    "workdir",
    "priority",
    "dependency",
]
SQUEUE_FORMAT = "%i|%P|%j|%u|%T|%M|%l|%D|%R|%q|%V|%S|%C|%b|%Z|%Q|%E"

SACCT_FIELDS = [
    "jobid",
    "name",
    "state",
    "partition",
    "qos",
    "nodelist",
    "elapsed",
    "timelimit",
    "submit",
    "start",
    "end",
    "req_tres",
    "eligible",
    "workdir",
    "exitcode",
]
SACCT_FORMAT = (
    "JobID,JobName,State,Partition,QOS,NodeList,Elapsed,Timelimit,Submit,Start,End,"
    "ReqTRES,Eligible,WorkDir,ExitCode"
)

SSHARE_FIELDS = ["account", "user", "raw_shares", "norm_shares", "raw_usage", "norm_usage", "eff_usage",
                 "fairshare", "level_fs"]
SSHARE_FORMAT = "Account,User,RawShares,NormShares,RawUsage,NormUsage,EffectvUsage,FairShare,LevelFS"
# sprio 的各项都是"已乘权重"的分数，加起来就是 priority
PRIO_FACTORS = ["age", "assoc", "fairshare", "jobsize", "partition", "qos", "site"]
SPRIO_FIELDS = ["jobid", "pname", "user", "priority"] + PRIO_FACTORS + ["qos_name"]  # pname：分区名（partition 是分区因子）
SPRIO_FORMAT = "%i|%r|%u|%Y|%A|%B|%F|%J|%P|%Q|%S|%n"
SPRIO_W_FORMAT = "%A|%B|%F|%J|%P|%Q|%S"

QOS_LIMIT_FIELDS = ["name", "max_jobs_pu", "max_submit_pu", "max_wall", "max_tres_pu"]
QOS_LIMIT_FORMAT = "Name,MaxJobsPU,MaxSubmitPU,MaxWall,MaxTRESPU"


class Slurm(Scheduler):
    kind = "slurm"
    qos_label = "QoS"
    HISTORY_SECTION = "SACCT"
    PARTITION_LOAD = True
    FEATURES = frozenset({"history", "cluster_load", "qos_info", "backlog", "est_start",
                          "partition_info", "fairshare"})

    def steps(self, want):
        steps = [("SQUEUE", f"squeue --me -h -o '{SQUEUE_FORMAT}'")]
        if self.has("history"):
            steps.append(("SACCT", f"sacct -X --starttime=now-{want['history_hours']}hours "
                                   f"-o {SACCT_FORMAT} --parsable2 --noheader"))
        if want.get("load"):
            # 全集群（所有用户）每个 QoS × 分区的 running/pending 数，用来判断哪里比较空
            steps.append(("LOAD", "squeue -h -t RUNNING,PENDING -o '%q|%P|%T' | sort | uniq -c"))
        if want.get("partition_info"):
            # 分区|可用|时限|节点 已用/空闲/其他/总数|CPU 已用/空闲/其他/总数
            steps.append(("PART", "sinfo -h -o '%P|%a|%l|%F|%C'"))
            # 每个分区允许哪些 QoS、分区自己的 QoS、总节点/核数（有的集群不让 sinfo，但让 scontrol）
            steps.append(("PINFO", "scontrol show partition -o"))
        if want.get("qos_info"):
            steps.append(("ASSOC", "sacctmgr -nP show assoc user=$USER format=qos"))
            steps.append(("QOS", f"sacctmgr -nP show qos format={QOS_LIMIT_FORMAT}"))
        if want.get("fairshare"):
            steps.append(("SSHARE", f"sshare -U -P -n -o {SSHARE_FORMAT}"))
            steps.append(("SPRIO_W", f"sprio -w -o '{SPRIO_W_FORMAT}'"))
            # 所有人排队中的 job 的优先级（每个候选分区一行），和排队原因（等依赖 / 被 hold 的不算在前面）
            steps.append(("SPRIO", f"sprio -h -o '{SPRIO_FORMAT}'"))
            steps.append(("PDREASON", "squeue --all -h -t PENDING -o '%A|%r'"))
        if want.get("backlog"):
            steps.append(("ME", "id -un"))
            steps.extend(backlog.COMMANDS)
        return steps

    def required_sections(self):
        return {"SQUEUE", "SACCT"} if self.has("history") else {"SQUEUE"}

    def optional_sections(self):
        # 比如 bluebear：sinfo 报 slurm_load_node: Access/permission denied
        return {"PART", "PINFO", "SSHARE", "SPRIO_W", "SPRIO", "PDREASON"}

    def parse_jobs(self, sections, prev_jobs):
        name = self.cluster["name"]
        hist = parse_sacct(sections.get("SACCT", []), name)
        active = parse_squeue(sections.get("SQUEUE", []), name)
        jobs = {}
        for jid in list(hist) + [j for j in active if j not in hist]:
            rec = dict(hist.get(jid, {}))
            # squeue(活跃)覆盖 sacct(历史)，但不拿空值覆盖（比如 sacct 才有的 end）
            rec.update({k: v for k, v in active.get(jid, {}).items() if v not in ("", [])})
            rec["live"] = jid in active
            rec.setdefault("deps", [])
            jobs[jid] = rec
        return jobs

    def parse_load(self, sections):
        """'  3506 freecpu|cpu|PENDING' -> {"freecpu": {"running": 0, "pending": 3506}}"""
        return parse_load(sections.get("LOAD", []), by="qos")

    def parse_combo_load(self, sections):
        return parse_load(sections.get("LOAD", []), by="combo")

    def parse_partitions(self, sections):
        """scontrol（规则、总数，顺序以它为准）+ sinfo（已用 / 空闲）；两个都拿不到返回 None"""
        rows = {}
        for line in sections.get("PINFO", []):
            kv = dict(tok.split("=", 1) for tok in line.split() if "=" in tok)
            name = kv.get("PartitionName")
            if not name:
                continue
            allow = kv.get("AllowQos", "ALL")
            rows[name] = {
                "name": name, "default": kv.get("Default") == "YES",
                "avail": kv.get("State", "").lower(),
                "time_limit": "infinite" if kv.get("MaxTime") == "UNLIMITED" else kv.get("MaxTime", ""),
                "nodes": {"total": int(kv["TotalNodes"])} if kv.get("TotalNodes", "").isdigit() else None,
                "cpus": {"total": int(kv["TotalCPUs"])} if kv.get("TotalCPUs", "").isdigit() else None,
                "allow_qos": None if allow in ("ALL", "") else allow.split(","),
                "deny_qos": [] if kv.get("DenyQos", "") in ("", "(null)") else kv["DenyQos"].split(","),
                "part_qos": "" if kv.get("QoS", "N/A") == "N/A" else kv["QoS"],
            }
        for line in sections.get("PART", []):
            parts = line.split("|")
            if len(parts) != 5:
                continue
            name, avail, limit, nodes, cpus = parts
            name, default = name.rstrip("*"), name.endswith("*")
            row = rows.setdefault(name, {"name": name, "allow_qos": None, "deny_qos": [], "part_qos": ""})
            row.update({"default": default or row.get("default", False), "avail": avail,
                        "time_limit": limit or row.get("time_limit", "")})
            # sinfo 有已用 / 空闲就用 sinfo 的，没有就保留 scontrol 的总数
            row["nodes"] = split_counts(nodes) or row.get("nodes")
            row["cpus"] = split_counts(cpus) or row.get("cpus")
        return list(rows.values()) or None

    def parse_qos_info(self, sections):
        return {"assoc": parse_assoc(sections.get("ASSOC", [])),
                "limits": parse_qos_limits(sections.get("QOS", []))}

    def parse_fairshare(self, sections, jobs):
        return parse_fairshare(sections, jobs)

    def analyze_backlog(self, sections):
        me = (sections.get("ME") or [""])[0]
        return backlog.analyze(sections, me)

    def estimate_job(self, job, model):
        return backlog.estimate_job(job, model)


def parse_dependency(dep_raw):
    """squeue 的 %E 字段形如 'afterok:12345(unfulfilled),afterany:12346_*' 或 '(null)'。
    返回 [{"type": "afterok", "id": "12345", "status": "unfulfilled"}, ...]"""
    if not dep_raw or dep_raw.strip() in ("(null)", "", "N/A"):
        return []
    deps = []
    for item in re.split(r"[,?]", dep_raw):
        m = re.match(r"(\w+)(?::(.+?))?(?:\((\w+)\))?$", item.strip())
        if not m:
            continue
        dep_type, ids_str, status = m.group(1), m.group(2) or "", m.group(3) or ""
        ids_str = re.sub(r"\+\d+", "", ids_str)  # after:123+60 里的 +60 是分钟数
        for dep_id in dict.fromkeys(re.findall(r"(\d+)(?:_[\d*]+)?", ids_str)):
            deps.append({"type": dep_type, "id": dep_id, "status": status})
        if not ids_str:  # singleton 之类不带 id 的
            deps.append({"type": dep_type, "id": "", "status": status})
    return deps


def parse_squeue(lines, cluster_name):
    jobs = {}
    for rec in parse_lines(lines, SQUEUE_FIELDS):
        state = rec["state"]
        jobs[rec["jobid"]] = {
            "id": rec["jobid"],
            "cluster": cluster_name,
            "name": rec["name"],
            "partition": rec["partition"],
            "qos": rec["qos"],
            "state": state,
            "time_used": rec["time"],
            "time_limit": rec["timelimit"],
            "nnodes": rec["nnodes"],
            "nodelist": rec["nodelist_reason"] if state in ACTIVE else "",
            "reason": rec["nodelist_reason"].strip("()") if state not in ACTIVE else "",
            "submit": clean_time(rec["submit"]),
            "start": clean_time(rec["start"]),  # PENDING 时是 Slurm 预计的开始时间
            "end": "",
            "cpus": rec["cpus"],
            "tres": rec["tres"] if rec["tres"] not in ("N/A", "(null)") else "",
            "workdir": rec["workdir"],
            "priority": rec["priority"],
            "dependency_raw": rec["dependency"] if rec["dependency"] != "(null)" else "",
            "deps": parse_dependency(rec["dependency"]),
            "source": "squeue",
        }
    return jobs


def parse_sacct(lines, cluster_name):
    jobs = {}
    for rec in parse_lines(lines, SACCT_FIELDS):
        jobid = rec["jobid"]
        if "." in jobid:  # 保险起见再过滤一次 step（正常 -X 已经排除）
            continue
        state = rec["state"].split(" ")[0]  # "CANCELLED by 12345" -> "CANCELLED"
        jobs[jobid] = {
            "id": jobid,
            "cluster": cluster_name,
            "name": rec["name"],
            "partition": rec["partition"],
            "qos": rec["qos"],
            "state": state,
            "time_used": rec["elapsed"],
            "time_limit": rec["timelimit"],
            "nnodes": "",
            "nodelist": rec["nodelist"] if rec["nodelist"] != "None assigned" else "",
            "reason": "",
            "submit": clean_time(rec["submit"]),
            "start": clean_time(rec["start"]),
            "end": clean_time(rec["end"]),
            "workdir": rec["workdir"],
            "req_tres": rec["req_tres"],
            "eligible": clean_time(rec["eligible"]),
            "dependency_raw": "",
            "deps": [],
            "source": "sacct",
            "exit_code": rec["exitcode"],  # "退出码:信号"，比如 1:0
        }
    return jobs


def parse_load(lines, by="qos"):
    """'  3506 freecpu|cpu|PENDING' -> 按 QoS（by="qos"）或按 (QoS, 分区)（by="combo"）数 running/pending。
    排队中的 job 的分区可能是 'a,b,c'（哪个先有空位就去哪），按分区数时每个都算一次"""
    load = {}
    for line in lines:
        m = re.match(r"(\d+)\s+(.*)\|(.*)\|(\w+)$", line)
        if not m:
            continue
        count, qos, parts, state = int(m.group(1)), m.group(2), m.group(3), m.group(4)
        for key in ([qos] if by == "qos" else [(qos, p) for p in parts.split(",") if p]):
            entry = load.setdefault(key, {"running": 0, "pending": 0})
            if state == "RUNNING":
                entry["running"] += count
            elif state == "PENDING":
                entry["pending"] += count
    return load


def split_counts(s):
    """sinfo 的 'A/I/O/T' -> {"alloc", "idle", "other", "total"}"""
    vals = s.split("/")
    if len(vals) != 4 or not all(v.isdigit() for v in vals):
        return None
    return dict(zip(("alloc", "idle", "other", "total"), map(int, vals)))


def parse_assoc(lines):
    names = []
    for line in lines:
        for q in line.split(","):
            q = q.strip()
            if q and q not in names:
                names.append(q)
    return names


def parse_qos_limits(lines):
    limits = {}
    for rec in parse_lines(lines, QOS_LIMIT_FIELDS):
        limits[rec.pop("name")] = rec
    return limits


def num(v):
    """空 / inf / nan -> None（inf 写进 JSON 会变成 Infinity，浏览器解析不了，整个页面挂掉）"""
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def not_competing(reason):
    """排队但现在不参与排序的：等依赖、被 hold、还没到开始时间"""
    return reason.startswith("Dependency") or "held" in reason.lower() or reason == "BeginTime"


def prio_hist(prios, bins=24):
    """别人优先级的分布（网页画直方图用）：对数刻度等宽的 bins 个格子
    {"edges": [bins+1 个边界], "counts": [bins 个数]}"""
    lo, hi = max(min(prios), 1), max(max(prios), 2)
    if hi <= lo:
        hi = lo + 1
    a, b = math.log(lo), math.log(hi)
    counts = [0] * bins
    for p in prios:
        counts[min(int((math.log(max(p, lo)) - a) / (b - a) * bins), bins - 1)] += 1
    return {"edges": [round(math.exp(a + (b - a) * k / bins)) for k in range(bins + 1)], "counts": counts}


def parse_fairshare(sections, jobs):
    accounts = [{**{k: num(v) for k, v in rec.items()}, "account": rec["account"], "user": rec["user"]}
                for rec in parse_lines(sections.get("SSHARE", []), SSHARE_FIELDS)]
    weights = {}
    for line in sections.get("SPRIO_W", []):
        vals = line.split("|")
        if len(vals) == len(PRIO_FACTORS) and all(v.strip().isdigit() or not v.strip() for v in vals):
            weights = {k: int(v) for k, v in zip(PRIO_FACTORS, vals) if v.strip()}
    rows = parse_lines(sections.get("SPRIO", []), SPRIO_FIELDS)
    if not accounts and not rows:
        return None
    for r in rows:
        for k in ["priority"] + PRIO_FACTORS:
            r[k] = int(num(r[k]) or 0)
    mine = {jid for jid, j in jobs.items() if j["state"] in PENDING}
    me = {a["user"] for a in accounts} | {r["user"] for r in rows if r["jobid"] in mine}
    idle = {line.split("|", 1)[0] for line in sections.get("PDREASON", [])
            if "|" in line and not_competing(line.split("|", 1)[1])}

    # 别人参与排序的 job：每个分区按 (优先级高, jobid 小) 排好；每个用户在每个分区最靠前的位置
    queue, best, users = {}, {}, {}
    for r in rows:
        if r["user"] in me:
            continue
        users[r["user"]] = max(users.get(r["user"], 0), r["fairshare"])
        if r["jobid"] in idle:
            continue
        key = (-r["priority"], sort_jobid(r["jobid"]))
        queue.setdefault(r["pname"], []).append(key)
        b = best.setdefault(r["pname"], {})
        b[r["user"]] = min(b.get(r["user"], key), key)
    for keys in queue.values():
        keys.sort()

    out = {}
    for r in rows:
        if r["jobid"] not in mine:
            continue
        e = out.setdefault(r["jobid"], {"priority": r["priority"], "qos_name": r["qos_name"],
                                        "factors": {k: r[k] for k in PRIO_FACTORS}, "parts": {}})
        p, key = r["pname"], (-r["priority"], sort_jobid(r["jobid"]))
        e["parts"][p] = {"priority": r["priority"], "ahead": bisect_left(queue.get(p, []), key),
                         "users_ahead": sum(1 for k in best.get(p, {}).values() if k < key),
                         "competing": len(queue.get(p, []))}
        if r["priority"] > e["priority"]:  # 分区因子不同时各分区优先级不一样，记最高的那个
            e.update(priority=r["priority"], factors={k: r[k] for k in PRIO_FACTORS})
    my_fs = max([r["fairshare"] for r in rows if r["user"] in me] or [0])
    # 每个分区别人参与排序的 job：几个、几个人、优先级中位数（和你的比）
    parts = {p: {"competing": len(keys), "users": len(best[p]), "median": -keys[len(keys) // 2][0],
                 "hist": prio_hist([-k[0] for k in keys])}
             for p, keys in queue.items()}
    return {"accounts": accounts, "weights": weights, "sees_others": bool(users),
            "users": {"with_pending": len(users), "higher_fairshare": sum(v > my_fs for v in users.values()),
                      "my_fairshare_points": my_fs},
            "partitions": parts, "jobs": out}
