"""
(Son of) Grid Engine：qstat / qstat -j / accounting 文件
=======================================================
在旧的 MMM Hub Young（SGE 8.1.9）上试出来的：
- 自己的 job：`qstat -u $USER -r -xml`（状态、提交/开始时间、slots、PE、h_rt、gpu）
- 再对这些 job 跑一次 `qstat -j id,id,...`，只留几行：project、工作目录、依赖（-hold_jid）、提交时间
- 已经结束的 job：SGE 没有 sacct，qacct 要扫整个 accounting 文件（几个 GB，很慢），
  所以直接 tail 这个文件的最后几十 MB，用 awk 挑出自己最近结束的
  （config.yaml 里可以用 accounting_file / accounting_tail_mb 调整）
- 全集群：`qstat -u '*' -ext` 按 project × 状态计数（有的站点看不到别人的 job，会自动隐藏）
- 没有：QoS 式的限额（sacctmgr）、调度器自己的预计开始时间、排队积压估计

QoS 面板按 project（Free / Gold ...）分组；"分区"一列用并行环境（mpi-w12a），没有 PE 的用队列名。
SGE 没有"可以开始排队的时间"（eligible），这里用依赖的 job 里最后一个结束的时间来补；
依赖的 job 结束后 qstat -j 里就看不到这条依赖了，所以会沿用上一次刷新记下的依赖。
"""

import re
import xml.etree.ElementTree as ET
from datetime import datetime

from .base import ACTIVE, PENDING, Scheduler, fmt_duration

DETAIL_KEYS = ("job_number", "submission_time", "project", "cwd", "jid_predecessor_list")

# accounting(5) 的列（8.1.9）；按位置取，只用前面这些
ACCT = {"qname": 0, "hostname": 1, "owner": 3, "job_name": 4, "job_number": 5,
        "submission_time": 8, "start_time": 9, "end_time": 10, "failed": 11,
        "exit_status": 12, "ru_wallclock": 13, "project": 31, "granted_pe": 33,
        "slots": 34, "task_number": 35, "category": 39, "pe_taskid": 41}


class SGE(Scheduler):
    kind = "sge"
    qos_label = "Project"
    FEATURES = frozenset({"history", "cluster_load"})

    def steps(self, want):
        steps = [
            ("MINE", "qstat -u $USER -r -xml"),
            # 结尾的 true：一个 job 都没有时 grep 没匹配也不算失败
            ("DETAIL", "{ ids=$(qstat -u $USER | awk 'NR>2 && $1 ~ /^[0-9]+$/ {print $1}' | sort -u "
                       "| paste -sd, -); [ -z \"$ids\" ] || qstat -j \"$ids\" | grep -E '^("
                       + "|".join(DETAIL_KEYS) + "):'; true; }"),
        ]
        if self.has("history"):
            path = self.cluster.get("accounting_file") or "${SGE_ROOT}/${SGE_CELL:-default}/common/accounting"
            mb = int(self.cluster.get("accounting_tail_mb", 30))
            hours = int(want["history_hours"])
            steps.append(("HIST", f"f=\"{path}\"; [ -r \"$f\" ] && tail -c {mb * 1000000} \"$f\" | "
                                  f"awk -F: -v u=\"$(id -un)\" -v t=$(( $(date +%s) - {hours * 3600} )) "
                                  "'$4==u && ($11>=t || ($11==0 && $9>=t))'"))
        if want.get("load"):
            steps.append(("LOAD", "qstat -u '*' -ext | awk 'NR>2 && $1 ~ /^[0-9]+$/ {print $6\"|\"$8}' "
                                  "| sort | uniq -c"))
        return steps

    def required_sections(self):
        return {"MINE"}

    def parse_jobs(self, sections, prev_jobs):
        name = self.cluster["name"]
        details = parse_detail(sections.get("DETAIL", []))
        active = parse_qstat_xml(sections.get("MINE", []), name, details)
        hist = parse_accounting(sections.get("HIST", []), name)
        prev_jobs = prev_jobs or {}
        jobs = {}
        for jid in list(hist) + [j for j in active if j not in hist]:
            rec = dict(hist.get(jid, {}))
            rec.update({k: v for k, v in active.get(jid, {}).items() if v not in ("", [])})
            rec["live"] = jid in active
            rec.setdefault("deps", [])
            # 历史记录里没有工作目录 / 依赖，用上一次在队列里看到它时记下的
            prev = prev_jobs.get(jid) or {}
            for k in ("workdir", "qos", "submit", "partition", "dependency_raw", "eligible"):
                if not rec.get(k) and prev.get(k):
                    rec[k] = prev[k]
            have = {d["id"] for d in rec["deps"]}
            rec["deps"] = rec["deps"] + [d for d in prev.get("deps", []) if d["id"] not in have]
            jobs[jid] = rec
        # eligible = 提交时间，或者依赖的 job 里最后一个结束的时间。
        # 只在历史记录里出现、从没在队列里见过的 job 不知道有没有依赖，留空
        for jid, j in jobs.items():
            if j.get("eligible"):
                continue
            if not j["deps"]:
                if j["live"] or jid in prev_jobs:
                    j["eligible"] = j.get("submit", "")
                continue
            ends = [(jobs.get(d["id"]) or prev_jobs.get(d["id"]) or {}).get("end") for d in j["deps"]]
            if all(ends):
                j["eligible"] = max([j.get("submit", "")] + ends)
        return jobs

    def parse_load(self, sections):
        """'  12 Gold|qw' -> {"Gold": {"running": 0, "pending": 12}}"""
        load = {}
        for line in sections.get("LOAD", []):
            m = re.match(r"(\d+)\s+(.*)\|(\S+)$", line)
            if not m:
                continue
            state = map_state(m.group(3))
            entry = load.setdefault(m.group(2), {"running": 0, "pending": 0})
            if state in ACTIVE:
                entry["running"] += int(m.group(1))
            elif state in PENDING:
                entry["pending"] += int(m.group(1))
        return load


# ---------------- 解析 ----------------
def map_state(code):
    """SGE 状态字母 -> Slurm 的状态名。d 删除中 E 出错 h 挂起(hold/依赖) q 排队 r 运行
    R 重启过 s/S/T 暂停 t 传输中(刚开始)"""
    if "s" in code or "S" in code or "T" in code:
        return "SUSPENDED"
    if "q" in code:
        return "PENDING"  # qw / hqw / Eqw / Rq
    if "d" in code:
        return "COMPLETING"
    if "t" in code:
        return "CONFIGURING"
    if "r" in code:
        return "RUNNING"
    return "PENDING"


def pending_reason(code, deps):
    if "E" in code:
        return f"Error ({code})"
    if "h" in code:
        return f"Dependency ({code})" if deps else f"Hold ({code})"
    if "R" in code:
        return f"Requeued ({code})"
    return code


def short_host(h):
    return (h or "").split(".")[0]


def parse_sge_time(s):
    """'Tue Oct 20 12:28:58 2026' 或 '2026-10-20T12:28:58' -> '2026-10-20T12:28:58'"""
    s = (s or "").strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%a %b %d %H:%M:%S %Y"):
        try:
            return datetime.strptime(s[:24] if "T" not in s else s[:19], fmt).strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue
    return ""


def epoch_to_local(v):
    try:
        t = float(v)
    except (TypeError, ValueError):
        return ""
    if t <= 0:
        return ""
    if t > 1e11:  # 有的版本记的是毫秒
        t /= 1000
    return datetime.fromtimestamp(t).strftime("%Y-%m-%dT%H:%M:%S")


def parse_detail(lines):
    """qstat -j 的输出（只留了 DETAIL_KEYS 那几行）-> {jobid: {key: value}}"""
    out, cur = {}, None
    for line in lines:
        key, _, val = line.partition(":")
        key, val = key.strip(), val.strip()
        if key == "job_number":
            cur = out.setdefault(val, {})
        elif cur is not None and key in DETAIL_KEYS:
            cur[key] = val
    return out


def parse_qstat_xml(lines, cluster_name, details):
    text = "\n".join(lines)
    start = text.find("<?xml")
    if start < 0:
        start = text.find("<job_info")
    if start < 0:
        return {}
    try:
        root = ET.fromstring(text[start:])
    except ET.ParseError:
        return {}
    now = datetime.now()
    jobs = {}
    for jl in root.iter("job_list"):
        g = lambda tag: (jl.findtext(tag) or "").strip()  # noqa: E731
        num = g("JB_job_number")
        if not num:
            continue
        tasks = g("tasks")
        jid = num if not tasks else (f"{num}_{tasks}" if tasks.isdigit() else f"{num}_[{tasks}]")
        code = g("state")
        state = map_state(code)
        det = details.get(num, {})
        reqs = {e.get("name"): (e.text or "").strip() for e in jl.findall("hard_request")}
        rpe, gpe = jl.find("requested_pe"), jl.find("granted_pe")
        queue, _, host = g("queue_name").partition("@")
        pe = (gpe.get("name") if gpe is not None else "") or (rpe.get("name") if rpe is not None else "")
        start = parse_sge_time(g("JAT_start_time")) if state in ACTIVE else ""
        submit = parse_sge_time(g("JB_submission_time")) or parse_sge_time(det.get("submission_time"))
        deps = [{"type": "hold_jid", "id": d, "status": ""}
                for d in dict.fromkeys(re.findall(r"\d+", det.get("jid_predecessor_list", "")))]
        slots = g("slots")
        gpus = reqs.get("gpu", "")
        tres = [f"cpu={slots}"] if slots else []
        if gpus and gpus != "0":
            tres.append(f"gres/gpu={gpus}")
        if reqs.get("memory"):
            tres.append(f"mem={reqs['memory']}/slot")
        used = ""
        if start:
            used = fmt_duration((now - datetime.strptime(start, "%Y-%m-%dT%H:%M:%S")).total_seconds())
        jobs[jid] = {
            "id": jid,
            "cluster": cluster_name,
            "name": g("full_job_name") or g("JB_name"),
            "partition": pe or queue,
            "qos": det.get("project", ""),
            "state": state,
            "time_used": used,
            "time_limit": fmt_duration(reqs["h_rt"]) if reqs.get("h_rt", "").isdigit() else reqs.get("h_rt", ""),
            "nnodes": "",
            "nodelist": short_host(host) if state in ACTIVE else "",
            "reason": "" if state in ACTIVE else pending_reason(code, deps),
            "submit": submit,
            "start": start,
            "end": "",
            "cpus": slots,
            "tres": "",
            "req_tres": ",".join(tres),
            "workdir": det.get("cwd", ""),
            "priority": g("JAT_prio"),
            "dependency_raw": ("hold_jid " + ",".join(d["id"] for d in deps)) if deps else "",
            "deps": deps,
            "source": "qstat",
            "sge_state": code,
        }
    return jobs


def acct_state(rec, h_rt):
    started = rec["start_time"] not in ("", "0")
    try:
        exit_status, failed = int(rec["exit_status"]), int(rec["failed"])
        wall = float(rec["ru_wallclock"])
    except ValueError:
        return "FAILED"
    if not started:
        return "CANCELLED"  # 排队时被删掉了
    if h_rt and wall >= h_rt - 5 and (exit_status or failed):
        return "TIMEOUT"
    if exit_status == 0 and failed == 0:
        return "COMPLETED"
    if exit_status in (137, 143) or failed == 100:  # 被 SIGKILL/SIGTERM：qdel 或者超了别的限额
        return "CANCELLED"
    return "FAILED"


def parse_accounting(lines, cluster_name):
    jobs = {}
    for line in lines:
        parts = line.split(":")
        if len(parts) <= ACCT["pe_taskid"]:
            continue
        rec = {k: parts[i] for k, i in ACCT.items()}
        if rec["pe_taskid"] not in ("NONE", ""):  # 并行 job 每个从节点的记录，只要主记录
            continue
        num, task = rec["job_number"], rec["task_number"]
        jid = num if task in ("0", "", "undefined") else f"{num}_{task}"
        m = re.search(r"h_rt=(\d+)", rec["category"])
        h_rt = int(m.group(1)) if m else 0
        gm = re.search(r"\bgpu=(\d+)", rec["category"])
        end = epoch_to_local(rec["end_time"])
        prev = jobs.get(jid)
        if prev and prev["end"] >= end:  # 被重新调度过的 job 会有好几条，留最后一条
            continue
        tres = [f"cpu={rec['slots']}"]
        if gm and gm.group(1) != "0":
            tres.append(f"gres/gpu={gm.group(1)}")
        pe = rec["granted_pe"] if rec["granted_pe"] not in ("NONE", "") else ""
        jobs[jid] = {
            "id": jid,
            "cluster": cluster_name,
            "name": rec["job_name"],
            "partition": pe or rec["qname"],
            "qos": rec["project"] if rec["project"] not in ("NONE", "") else "",
            "state": acct_state(rec, h_rt),
            "time_used": fmt_duration(rec["ru_wallclock"]),
            "time_limit": fmt_duration(h_rt) if h_rt else "",
            "nnodes": "",
            "nodelist": short_host(rec["hostname"]) if rec["start_time"] not in ("", "0") else "",
            "reason": "",
            "submit": epoch_to_local(rec["submission_time"]),
            "start": epoch_to_local(rec["start_time"]),
            "end": end,
            "cpus": rec["slots"],
            "tres": "",
            "req_tres": ",".join(tres),
            "workdir": "",
            "priority": "",
            "dependency_raw": "",
            "deps": [],
            "source": "accounting",
            "exit_status": rec["exit_status"],
        }
    return jobs
