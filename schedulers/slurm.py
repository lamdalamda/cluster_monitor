"""Slurm：squeue / sacct / sacctmgr / sinfo"""

import re

import backlog

from .base import ACTIVE, Scheduler, clean_time, parse_lines

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
]
SACCT_FORMAT = (
    "JobID,JobName,State,Partition,QOS,NodeList,Elapsed,Timelimit,Submit,Start,End,"
    "ReqTRES,Eligible,WorkDir"
)

QOS_LIMIT_FIELDS = ["name", "max_jobs_pu", "max_submit_pu", "max_wall", "max_tres_pu"]
QOS_LIMIT_FORMAT = "Name,MaxJobsPU,MaxSubmitPU,MaxWall,MaxTRESPU"


class Slurm(Scheduler):
    kind = "slurm"
    qos_label = "QoS"
    FEATURES = frozenset({"history", "cluster_load", "qos_info", "backlog", "est_start"})

    def steps(self, want):
        steps = [("SQUEUE", f"squeue --me -h -o '{SQUEUE_FORMAT}'")]
        if self.has("history"):
            steps.append(("SACCT", f"sacct -X --starttime=now-{want['history_hours']}hours "
                                   f"-o {SACCT_FORMAT} --parsable2 --noheader"))
        if want.get("load"):
            # 全集群（所有用户）每个 QoS 的 running/pending 数，用来判断哪里比较空
            steps.append(("LOAD", "squeue -h -t RUNNING,PENDING -o '%q|%T' | sort | uniq -c"))
        if want.get("qos_info"):
            steps.append(("ASSOC", "sacctmgr -nP show assoc user=$USER format=qos"))
            steps.append(("QOS", f"sacctmgr -nP show qos format={QOS_LIMIT_FORMAT}"))
        if want.get("backlog"):
            steps.append(("ME", "id -un"))
            steps.extend(backlog.COMMANDS)
        return steps

    def required_sections(self):
        return {"SQUEUE", "SACCT"} if self.has("history") else {"SQUEUE"}

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
        """'  3506 freecpu|PENDING' -> {"freecpu": {"running": 0, "pending": 3506}}"""
        return parse_load(sections.get("LOAD", []))

    def parse_qos_info(self, sections):
        return {"assoc": parse_assoc(sections.get("ASSOC", [])),
                "limits": parse_qos_limits(sections.get("QOS", []))}

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
        }
    return jobs


def parse_load(lines):
    """'  3506 freecpu|PENDING' -> {"freecpu": {"running": 0, "pending": 3506}}"""
    load = {}
    for line in lines:
        m = re.match(r"(\d+)\s+(.*)\|(\w+)$", line)
        if not m:
            continue
        count, qos, state = int(m.group(1)), m.group(2), m.group(3)
        entry = load.setdefault(qos, {"running": 0, "pending": 0})
        if state == "RUNNING":
            entry["running"] += count
        elif state == "PENDING":
            entry["pending"] += count
    return load


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
