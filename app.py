#!/usr/bin/env python3
"""
HPC Job Dashboard
==================
在本机跑一个小 Flask 服务，后台线程定期通过 `ssh <host> squeue/sacct` 拉取
你在各个 Slurm 集群上的作业状态，解析 job 之间的 dependency 关系，
通过网页展示成一张状态图（node=job, edge=dependency）。

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
    "dependency",
]
SQUEUE_FORMAT = "%i|%P|%j|%u|%T|%M|%l|%D|%R|%E"

SACCT_FIELDS = ["jobid", "name", "state", "partition", "nodelist", "elapsed", "start", "end"]

# 结束态判定（用于图上着色 & 判断依赖是否已经满足）
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
    """跑一条远程命令，返回 (ok, stdout, stderr)"""
    try:
        proc = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", ssh_host, remote_cmd],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if proc.returncode != 0:
            return False, "", proc.stderr.strip()
        return True, proc.stdout, ""
    except subprocess.TimeoutExpired:
        return False, "", "ssh timeout"
    except Exception as e:  # noqa: BLE001
        return False, "", str(e)


def parse_dependency(dep_raw):
    """squeue 的 %E 字段形如 'afterok:12345,afterany:12346(unfulfilled)' 或 '(null)'。
    返回依赖的 job id 列表（字符串）。"""
    if not dep_raw or dep_raw.strip() in ("(null)", "", "N/A"):
        return []
    ids = re.findall(r"(\d+)(?:_\d+)?", dep_raw)
    return list(dict.fromkeys(ids))  # 去重保序


def parse_squeue(output, cluster_name):
    jobs = {}
    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("|")
        if len(parts) != len(SQUEUE_FIELDS):
            continue
        rec = dict(zip(SQUEUE_FIELDS, parts))
        jobid = rec["jobid"]
        state = rec["state"]
        nodelist = rec["nodelist_reason"] if state in ACTIVE else ""
        reason = rec["nodelist_reason"] if state not in ACTIVE else ""
        jobs[jobid] = {
            "id": jobid,
            "cluster": cluster_name,
            "name": rec["name"],
            "user": rec["user"],
            "partition": rec["partition"],
            "state": state,
            "time_used": rec["time"],
            "time_limit": rec["timelimit"],
            "nnodes": rec["nnodes"],
            "nodelist": nodelist,
            "reason": reason,
            "dependency_raw": rec["dependency"],
            "depends_on": parse_dependency(rec["dependency"]),
            "source": "squeue",
        }
    return jobs


def parse_sacct(output, cluster_name):
    jobs = {}
    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
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
            "user": "",
            "partition": rec["partition"],
            "state": state,
            "time_used": rec["elapsed"],
            "time_limit": "",
            "nnodes": "",
            "nodelist": rec["nodelist"] if state in ACTIVE | TERMINAL_OK else "",
            "reason": "",
            "dependency_raw": "",
            "depends_on": [],
            "start": rec["start"],
            "end": rec["end"],
            "source": "sacct",
        }
    return jobs


class Poller:
    def __init__(self, config):
        self.config = config
        self.lock = threading.Lock()
        self.state = {"jobs": [], "edges": [], "clusters": [], "updated_at": None, "errors": []}
        self._stop = False

    def poll_once(self):
        cfg = self.config
        timeout = cfg.get("ssh_timeout_seconds", 15)
        hours = cfg.get("history_hours", 48)

        all_jobs = {}
        errors = []
        cluster_status = []

        for c in cfg.get("clusters", []):
            if not c.get("enabled", True):
                continue
            name = c["name"]
            ssh_host = c["ssh_host"]

            ok1, out1, err1 = run_ssh(ssh_host, f"squeue --me -h -o '{SQUEUE_FORMAT}'", timeout)
            ok2, out2, err2 = run_ssh(
                ssh_host,
                f"sacct -X --starttime=now-{hours}hours "
                f"-o JobID,JobName,State,Partition,NodeList,Elapsed,Start,End "
                f"--parsable2 --noheader",
                timeout,
            )

            if not ok1 and not ok2:
                errors.append(f"{name}: {err1 or err2}")
                cluster_status.append({"name": name, "ok": False, "error": err1 or err2})
                continue

            cluster_status.append({"name": name, "ok": True, "error": None})

            hist = parse_sacct(out2, name) if ok2 else {}
            active = parse_squeue(out1, name) if ok1 else {}

            merged = {**hist, **active}  # squeue(活跃)覆盖 sacct(历史)
            for jid, rec in merged.items():
                all_jobs[f"{name}:{jid}"] = rec

        # 生成 edges：dependency -> job（同集群内）
        edges = []
        for key, rec in all_jobs.items():
            cluster = rec["cluster"]
            for dep_id in rec["depends_on"]:
                dep_key = f"{cluster}:{dep_id}"
                edges.append(
                    {
                        "from": dep_key,
                        "to": key,
                        "dep_satisfied": all_jobs.get(dep_key, {}).get("state") in TERMINAL_OK,
                    }
                )
                if dep_key not in all_jobs:
                    # 依赖的 job 既不在 squeue 也不在 sacct 历史窗口里，补一个占位节点
                    all_jobs[dep_key] = {
                        "id": dep_id,
                        "cluster": cluster,
                        "name": "(unknown / out of history window)",
                        "user": "",
                        "partition": "",
                        "state": "UNKNOWN",
                        "time_used": "",
                        "time_limit": "",
                        "nnodes": "",
                        "nodelist": "",
                        "reason": "",
                        "dependency_raw": "",
                        "depends_on": [],
                        "source": "placeholder",
                    }

        with self.lock:
            self.state = {
                "jobs": list(all_jobs.values()),
                "edges": edges,
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
