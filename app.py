#!/usr/bin/env python3
"""
HPC Job Dashboard
==================
在本机跑一个小 Flask 服务，后台线程定期通过 `ssh <host> squeue/sacct/sacctmgr`
拉取你在各个 Slurm 集群上的作业状态，网页上用表格展示：
- 同名 job（通常是一条 dependency 链 / 同一批提交）折叠成一行，点三角展开
- 每个集群一张 QoS 表：你在每个 QoS 上跑了/排了多少，全集群排队情况，QoS 限额

用法：
    pip install -r requirements.txt
    编辑 config.yaml 填好你的集群 ssh 别名
    python app.py
    浏览器打开 http://127.0.0.1:8787 (端口见 config.yaml)

依赖：本机能免密 ssh 到各集群登录节点（ssh key / agent），不处理交互式密码。
"""

import json
import re
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import yaml
from flask import Flask, jsonify, render_template

CONFIG_PATH = "config.yaml"

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
    "dependency",
]
SQUEUE_FORMAT = "%i|%P|%j|%u|%T|%M|%l|%D|%R|%q|%V|%S|%E"

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
]
SACCT_FORMAT = "JobID,JobName,State,Partition,QOS,NodeList,Elapsed,Timelimit,Submit,Start,End"

QOS_LIMIT_FIELDS = ["name", "max_jobs_pu", "max_submit_pu", "max_wall", "max_tres_pu"]
QOS_LIMIT_FORMAT = "Name,MaxJobsPU,MaxSubmitPU,MaxWall,MaxTRESPU"

# 结束态判定（用于着色 & 判断依赖是否已经满足）
TERMINAL_OK = {"COMPLETED"}
TERMINAL_BAD = {
    "FAILED",
    "CANCELLED",
    "TIMEOUT",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "BOOT_FAIL",
    "DEADLINE",
}
ACTIVE = {"RUNNING", "COMPLETING", "CONFIGURING"}
PENDING = {"PENDING", "SUSPENDED", "REQUEUED", "REQUEUE_HOLD", "RESV_DEL_HOLD"}


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def run_ssh(ssh_host, remote_cmd, timeout):
    """跑一条远程命令，返回 (ok, stdout, stderr)。
    ok 只表示 SSH 本身连上了（返回码 255 是 ssh 自己的错误），
    远程各条命令成功与否由 split_sections 里的 @@ERR 标记判断。"""
    try:
        proc = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", ssh_host, remote_cmd],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if proc.returncode == 255:
            return False, "", proc.stderr.strip()
        return True, proc.stdout, proc.stderr.strip()
    except subprocess.TimeoutExpired:
        return False, "", "ssh timeout"
    except Exception as e:  # noqa: BLE001
        return False, "", str(e)


def build_remote_cmd(hours, with_load, with_qos_info):
    """把所有要跑的命令拼成一次 SSH，用 @@SECTION 标记分段（省掉多次 SSH 握手）。
    某条命令失败时输出 @@ERR <section>，不影响其它段。"""
    steps = [
        ("SQUEUE", f"squeue --me -h -o '{SQUEUE_FORMAT}'"),
        (
            "SACCT",
            f"sacct -X --starttime=now-{hours}hours -o {SACCT_FORMAT} --parsable2 --noheader",
        ),
    ]
    if with_load:
        # 全集群（所有用户）每个 QoS 的 running/pending 数，用来判断哪里比较空
        steps.append(("LOAD", "squeue -h -t RUNNING,PENDING -o '%q|%T' | sort | uniq -c"))
    if with_qos_info:
        steps.append(("ASSOC", "sacctmgr -nP show assoc user=$USER format=qos"))
        steps.append(("QOS", f"sacctmgr -nP show qos format={QOS_LIMIT_FORMAT}"))
    parts = []
    for name, cmd in steps:
        parts.append(f"echo @@{name}; {cmd} || echo '@@ERR {name}'")
    parts.append("echo @@END")
    return "; ".join(parts)


def split_sections(output):
    """把 build_remote_cmd 的输出按 @@SECTION 切开。
    登录脚本（.bashrc 之类）在第一个标记前打印的东西会被丢掉。"""
    sections, failed = {}, set()
    current = None
    for line in output.splitlines():
        s = line.strip()
        if s.startswith("@@ERR "):
            failed.add(s[6:].strip())
            continue
        if s.startswith("@@") and s[2:].isupper():
            current = s[2:]
            sections[current] = []
            continue
        if current and s:
            sections[current].append(s)
    return sections, failed


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


def clean_time(v):
    return "" if v in ("N/A", "Unknown", "None", "NONE", "") else v


def parse_squeue(lines, cluster_name):
    jobs = {}
    for line in lines:
        parts = line.split("|")
        if len(parts) != len(SQUEUE_FIELDS):
            continue
        rec = dict(zip(SQUEUE_FIELDS, parts))
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
            "dependency_raw": rec["dependency"] if rec["dependency"] != "(null)" else "",
            "deps": parse_dependency(rec["dependency"]),
            "source": "squeue",
        }
    return jobs


def parse_sacct(lines, cluster_name):
    jobs = {}
    for line in lines:
        parts = line.split("|")
        if len(parts) != len(SACCT_FIELDS):
            continue
        rec = dict(zip(SACCT_FIELDS, parts))
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
    for line in lines:
        parts = line.split("|")
        if len(parts) != len(QOS_LIMIT_FIELDS):
            continue
        rec = dict(zip(QOS_LIMIT_FIELDS, parts))
        limits[rec.pop("name")] = rec
    return limits


def normalize_qos_config(raw):
    """config.yaml 里 qos 可以写成字符串，也可以写成 {name, note}"""
    out = []
    for item in raw or []:
        if isinstance(item, dict):
            if item.get("name"):
                out.append({"name": str(item["name"]), "note": str(item.get("note", "") or "")})
        elif item:
            out.append({"name": str(item), "note": ""})
    return out


class Poller:
    def __init__(self, config):
        self.config = config
        self.lock = threading.Lock()
        self.poll_lock = threading.Lock()  # 防止后台轮询和"立即刷新"同时跑
        self.state = {"jobs": [], "clusters": [], "updated_at": None, "errors": []}
        # sacctmgr 的 QoS 信息基本不变，缓存起来，不用每轮都查
        self.qos_info = {}  # cluster name -> {"assoc": [...], "limits": {...}, "fetched_at": t}
        self._stop = False

    def poll_cluster(self, c):
        cfg = self.config
        name = c["name"]
        timeout = cfg.get("ssh_timeout_seconds", 15)
        hours = cfg.get("history_hours", 48)
        with_load = cfg.get("show_cluster_load", True)
        refresh_s = cfg.get("qos_info_refresh_minutes", 30) * 60
        cached = self.qos_info.get(name)
        with_qos_info = cached is None or time.time() - cached["fetched_at"] > refresh_s

        ok, out, err = run_ssh(
            c["ssh_host"], build_remote_cmd(hours, with_load, with_qos_info), timeout
        )
        if not ok:
            return {"name": name, "ok": False, "error": err or "ssh failed", "warnings": [],
                    "jobs": {}, "qos": []}

        sections, failed = split_sections(out)
        warnings = []
        if "END" not in sections or {"SQUEUE", "SACCT"} <= failed:
            return {"name": name, "ok": False, "error": err[-300:] or "squeue/sacct 都失败了",
                    "warnings": [], "jobs": {}, "qos": []}
        for sec in sorted(failed):
            warnings.append(f"{sec.lower()} 命令失败")

        hist = parse_sacct(sections.get("SACCT", []), name)
        active = parse_squeue(sections.get("SQUEUE", []), name)
        jobs = {}
        for jid in list(hist) + [j for j in active if j not in hist]:
            rec = dict(hist.get(jid, {}))
            # squeue(活跃)覆盖 sacct(历史)，但不拿空值覆盖（比如 sacct 才有的 end）
            rec.update({k: v for k, v in active.get(jid, {}).items() if v not in ("", [])})
            rec.setdefault("deps", [])
            jobs[jid] = rec

        if with_qos_info:
            cached = {
                "assoc": parse_assoc(sections.get("ASSOC", [])),
                "limits": parse_qos_limits(sections.get("QOS", [])),
                "fetched_at": time.time(),
            }
            self.qos_info[name] = cached
        load = parse_load(sections.get("LOAD", [])) if with_load else {}
        # 有的集群（比如 PrivateData=jobs）squeue 只能看到自己的 job，
        # 这时"全集群"数字其实就是你自己的，没有参考价值，标记一下前端显示为隐藏
        mine_active = sum(1 for j in jobs.values() if j["state"] in ACTIVE | PENDING)
        load_total = sum(v["running"] + v["pending"] for v in load.values())
        load_private = with_load and load_total == mine_active
        if load_private:
            load, with_load = {}, False

        return {"name": name, "ok": True, "error": None, "warnings": warnings,
                "load_private": load_private,
                "jobs": jobs, "qos": self.build_qos_table(c, jobs, cached, load, with_load)}

    @staticmethod
    def build_qos_table(c, jobs, cached, load, with_load):
        """QoS 列表优先用 config.yaml 里写的；没写就用 sacctmgr assoc 查到的；
        再补上你当前 job 实际用到的 QoS。"""
        configured = normalize_qos_config(c.get("qos"))
        configured_names = {r["name"] for r in configured}
        rows = list(configured) or [{"name": q, "note": ""} for q in cached["assoc"]]
        seen = {r["name"] for r in rows}
        for rec in sorted(jobs.values(), key=lambda r: r["qos"]):
            q = rec.get("qos")
            if q and q not in seen:
                rows.append({"name": q, "note": ""})
                seen.add(q)

        table = []
        for r in rows:
            q = r["name"]
            mine = [j for j in jobs.values() if j.get("qos") == q]
            lim = cached["limits"].get(q, {})
            table.append({
                "name": q,
                "note": r["note"],
                "configured": q in configured_names,
                "mine_running": sum(1 for j in mine if j["state"] in ACTIVE),
                "mine_pending": sum(1 for j in mine if j["state"] in PENDING),
                "all_running": load.get(q, {}).get("running", 0) if with_load else None,
                "all_pending": load.get(q, {}).get("pending", 0) if with_load else None,
                "max_jobs_pu": lim.get("max_jobs_pu", ""),
                "max_submit_pu": lim.get("max_submit_pu", ""),
                "max_wall": lim.get("max_wall", ""),
                "max_tres_pu": lim.get("max_tres_pu", ""),
            })
        return table

    def poll_once(self):
        with self.poll_lock:
            clusters = [c for c in self.config.get("clusters", []) if c.get("enabled", True)]
            with ThreadPoolExecutor(max_workers=max(1, len(clusters))) as ex:
                results = list(ex.map(self.poll_cluster, clusters))

            all_jobs, errors, cluster_status = [], [], []
            for r in results:
                if not r["ok"]:
                    errors.append(f"{r['name']}: {r['error']}")
                errors.extend(f"{r['name']}: {w}" for w in r["warnings"])
                all_jobs.extend(r["jobs"].values())
                cluster_status.append({
                    "name": r["name"], "ok": r["ok"], "error": r["error"],
                    "load_private": r.get("load_private", False), "qos": r["qos"],
                })

            with self.lock:
                self.state = {
                    "jobs": all_jobs,
                    "clusters": cluster_status,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "errors": errors,
                }

    def loop(self):
        interval = self.config.get("poll_interval_seconds", 15)
        while not self._stop:
            try:
                self.poll_once()
            except Exception as e:  # noqa: BLE001
                with self.lock:
                    self.state["errors"] = [f"poll loop error: {e}"]
            time.sleep(interval)

    def get_state(self):
        with self.lock:
            return json.loads(json.dumps(self.state))

    def start(self):
        t = threading.Thread(target=self.loop, daemon=True)
        t.start()


app = Flask(__name__)
config = load_config()
poller = Poller(config)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/jobs")
def api_jobs():
    return jsonify(poller.get_state())


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    """手动触发一次立即刷新（不等下一个轮询周期）"""
    poller.poll_once()
    return jsonify({"ok": True})


if __name__ == "__main__":
    poller.poll_once()  # 启动时先同步跑一次，避免打开页面时是空的
    poller.start()
    app.run(host=config.get("host", "127.0.0.1"), port=config.get("port", 8787), debug=False)
