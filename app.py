#!/usr/bin/env python3
"""
HPC Job Dashboard
==================
在本机跑一个小 Flask 服务，通过 `ssh <host> squeue/sacct/sacctmgr/...`
拉取你在各个 Slurm 集群上的作业状态，网页上用表格展示：
- 同名 job（通常是一条 dependency 链 / 同一批提交）折叠成一行，点三角展开；
  展开时顺便看每个 job 工作目录里的文件（ls + tail vasp.out / Slab*.log）
- 每个集群一张 QoS 表：你在每个 QoS 上跑了/排了多少，全集群排队情况，QoS 限额
- 排队积压估计（backlog.py）

网页和 SSH 是解耦的：所有远程操作放进一个后台任务队列，一个接一个执行，
网页随时打开都是立刻显示（上一次的数据会存到 .dashboard_cache.json），
上方进度条显示后台正在干什么。默认不自动刷新，点"立即刷新"才去拉。

用法：
    pip install -r requirements.txt
    编辑 config.yaml 填好你的集群 ssh 别名
    python app.py
    浏览器打开 http://127.0.0.1:8787 (端口见 config.yaml)

依赖：本机能用 ssh key 登录各集群。需要 2FA 的集群（比如 sulis）在 2FA 还在
有效期内时也能用；过期了会显示"需要输入"，在终端里手动 ssh 登录一次再点"立即刷新"。
"""

import json
import os
import re
import subprocess
import threading
import time
from collections import deque
from datetime import datetime, timezone

import yaml
from flask import Flask, jsonify, render_template, request

import backlog
import jobfiles
import waitlog

APP_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(APP_DIR, "config.yaml")
ASKPASS = os.path.join(APP_DIR, "ssh_askpass_abort.sh")
CACHE_PATH = os.path.join(APP_DIR, ".dashboard_cache.json")
INSPECT_CHUNK = 8  # 每次 SSH 看几个目录
MAX_IDS_PER_DIR = 10  # 同一个目录里最多给几个 job 找各自的 slurm 输出文件

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


class NeedInput(Exception):
    """服务器要求交互输入（2FA 验证码 / 密码 / 确认 host key）"""


def run_ssh(ssh_host, remote_cmd, timeout):
    """跑一条远程命令，返回 (ok, stdout, stderr)。
    ok 只表示 SSH 本身连上了（返回码 255 是 ssh 自己的错误），
    远程各条命令成功与否由 split_sections 里的 @@ERR 标记判断。

    不用 BatchMode=yes：它会直接拒绝 keyboard-interactive 认证，而 sulis 这种
    "key + 2FA" 的集群，即使 2FA 还在有效期（不用输验证码），也要走一轮
    keyboard-interactive。这里允许它，但一旦服务器真的要输入东西，
    ssh_askpass_abort.sh 会直接杀掉 ssh（不发任何回答），抛 NeedInput。
    start_new_session 让 ssh 没有控制终端，不会在你跑 app.py 的终端里弹提示。"""
    env = dict(os.environ, SSH_ASKPASS=ASKPASS, SSH_ASKPASS_REQUIRE="force")
    env.setdefault("DISPLAY", "none")  # 老版本 OpenSSH 只有设了 DISPLAY 才会用 SSH_ASKPASS
    try:
        proc = subprocess.run(
            ["ssh", "-o", "BatchMode=no", "-o", "KbdInteractiveAuthentication=yes",
             "-o", "ConnectTimeout=10", ssh_host, remote_cmd],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
            start_new_session=True,
        )
    except subprocess.TimeoutExpired:
        return False, "", "ssh timeout"
    except Exception as e:  # noqa: BLE001
        return False, "", str(e)
    m = re.search(r"@@NEED_INPUT (.*)", proc.stderr)
    if m:
        raise NeedInput(m.group(1).strip() or "(prompt)")
    if proc.returncode == 255 or proc.returncode < 0:
        return False, "", proc.stderr.strip()[-300:]
    return True, proc.stdout, proc.stderr.strip()


def build_remote_cmd(steps):
    """把所有要跑的命令拼成一次 SSH，用 @@SECTION 标记分段（省掉多次 SSH 握手）。
    某条命令失败时输出 @@ERR <section>，不影响其它段。"""
    parts = [f"echo @@{name}; {cmd} || echo '@@ERR {name}'" for name, cmd in steps]
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


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def base_jid(jid):
    """'123_4' -> '123'（array job 的各个 task 算同一个 job）"""
    return jid.split("_")[0]


def dir_key(cluster, workdir):
    return f"{cluster}\u0000{workdir}"


class Backend:
    """所有 SSH 操作都放进 self.queue，由一个后台线程依次执行；网页只读 self 里的结果。"""

    def __init__(self, config):
        self.config = config
        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)
        self.queue = deque()  # 待执行任务 {"kind", "label", "steps", ...}
        self.current = None  # 正在执行的任务
        self.step_label = ""  # 正在执行的那一步
        self.batch_total = 0  # 这一批（从空闲到再次空闲）一共多少步，给进度条用
        self.batch_done = 0
        self.version = 0  # 数据每变一次 +1，前端据此决定要不要重新拉 /api/jobs
        self.last_refresh = None
        self.results = {}  # cluster name -> 给前端的集群结果（连不上时是旧数据 + 错误）
        self.last_good = {}  # cluster name -> 上一次成功的结果
        # 目录信息按 (集群, 目录) 存：多个 job 共用一个目录时只读一次
        self.inspect_dirs = {}  # dir_key -> {"dir", "exists", "size_kb", "ls", "files", "at"}
        self.inspect_job_err = {}  # "cluster:jobid" -> {"error", "at"}（比如没有工作目录信息）
        self.inspect_pending = set()  # 正在排队读取的 dir_key
        # sacctmgr 的 QoS 信息基本不变，缓存起来，不用每次都查
        self.qos_info = {}  # cluster name -> {"assoc": [...], "limits": {...}, "fetched_at": t}
        # 排队积压要拉全集群的 squeue，比较重，隔几分钟才拉一次
        self.backlog = {}  # cluster name -> {"result": {...}, "model": {...}, "fetched_at": t}
        # 需要 2FA 的集群：先停一阵子不自动重试（"立即刷新"会强制重试）
        self.auth_blocked = {}  # cluster name -> {"until": t, "error": str}
        # 每次刷新把请求资源 / 当时的集群状态 / 等待时间记到 waitlog/*.csv（以后建模用）
        self.waitlog = None
        if config.get("waitlog", True):
            self.waitlog = waitlog.WaitLog(
                os.path.join(APP_DIR, config.get("waitlog_dir", "waitlog")))
        self.load_cache()

    def clusters(self):
        return [c for c in self.config.get("clusters", []) if c.get("enabled", True)]

    def cluster_by_name(self, name):
        return next((c for c in self.clusters() if c["name"] == name), None)

    # ---------------- 磁盘缓存：重启后网页立刻有上次的数据 ----------------
    def load_cache(self):
        try:
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, ValueError):
            return
        self.last_good = data.get("last_good", {})
        self.results = data.get("results", {})
        self.inspect_dirs = data.get("inspect_dirs", {})
        self.inspect_job_err = data.get("inspect_job_err", {})
        self.qos_info = data.get("qos_info", {})
        self.backlog = data.get("backlog", {})
        self.last_refresh = data.get("last_refresh")

    def save_cache(self):
        with self.lock:
            data = json.dumps({
                "last_good": self.last_good, "results": self.results,
                "inspect_dirs": self.inspect_dirs, "inspect_job_err": self.inspect_job_err,
                "qos_info": self.qos_info, "backlog": self.backlog,
                "last_refresh": self.last_refresh,
            }, ensure_ascii=False)
        tmp = CACHE_PATH + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(data)
            os.replace(tmp, CACHE_PATH)
        except OSError:
            pass

    # ---------------- 任务队列 ----------------
    def _enqueue(self, task):
        # 调用方已持有 self.lock
        self.queue.append(task)
        self.batch_total += task["steps"]
        self.version += 1
        self.cv.notify()

    def enqueue_refresh(self, force=True):
        with self.lock:
            if any(t["kind"] == "refresh" for t in self.queue):
                return False  # 已经排着一个刷新了
            n = len(self.clusters())
            self._enqueue({"kind": "refresh", "label": "刷新作业状态", "steps": n, "force": force})
            return True

    def _jobs_of(self, cluster):
        return (self.last_good.get(cluster) or {}).get("jobs", {})

    def enqueue_inspect(self, keys, force=False):
        """keys: ["cluster:jobid", ...]。按目录去重：已经读过的目录（除非 force）
        和正在排队的目录跳过。"""
        todo = {}  # cluster -> {workdir: None}
        changed = False
        with self.lock:
            for key in dict.fromkeys(keys):
                cluster, _, jid = key.partition(":")
                if not self.cluster_by_name(cluster):
                    continue
                d = self._jobs_of(cluster).get(jid, {}).get("workdir", "")
                if not d:
                    self.inspect_job_err[key] = {
                        "error": "没有工作目录信息（squeue/sacct 里没查到 WorkDir）", "at": now_iso()}
                    changed = True
                    continue
                dk = dir_key(cluster, d)
                if dk in self.inspect_pending or (dk in self.inspect_dirs and not force):
                    continue
                todo.setdefault(cluster, {})[d] = None
            added = 0
            for cluster, dirs in todo.items():
                dirs = list(dirs)
                for i in range(0, len(dirs), INSPECT_CHUNK):
                    chunk = dirs[i:i + INSPECT_CHUNK]
                    self.inspect_pending.update(dir_key(cluster, d) for d in chunk)
                    self._enqueue({"kind": "inspect", "cluster": cluster, "dirs": chunk,
                                   "label": f"查看 {cluster} 上 {len(chunk)} 个目录",
                                   "steps": 1})
                    added += len(chunk)
            if changed and not added:
                self.version += 1
            return added

    def worker(self):
        while True:
            with self.lock:
                while not self.queue:
                    self.cv.wait()
                self.current = self.queue.popleft()
            task = self.current
            try:
                if task["kind"] == "refresh":
                    self.run_refresh(task)
                else:
                    self.run_inspect(task)
            except Exception as e:  # noqa: BLE001
                with self.lock:
                    self.results.setdefault("_errors", {})["worker"] = f"后台任务出错: {e}"
            finally:
                with self.lock:
                    self.current = None
                    self.step_label = ""
                    if not self.queue:
                        self.batch_total = self.batch_done = 0
                    self.version += 1
                self.save_cache()

    def _step(self, label):
        with self.lock:
            self.step_label = label
            self.version += 1

    def _step_done(self):
        with self.lock:
            self.batch_done += 1
            self.version += 1

    def run_refresh(self, task):
        clusters = self.clusters()
        for i, c in enumerate(clusters, 1):
            self._step(f"刷新 {c['name']}（{i}/{len(clusters)}）")
            r = self.poll_cluster(c, task.get("force", True))
            with self.lock:
                self.store_result(r)
            if r["ok"] and self.waitlog:
                try:
                    self.waitlog.update(r)
                    with self.lock:
                        self.results.get("_errors", {}).pop("waitlog", None)
                except Exception as e:  # noqa: BLE001  记录失败不影响刷新
                    with self.lock:
                        self.results.setdefault("_errors", {})["waitlog"] = f"写 waitlog 出错: {e}"
            self._step_done()
            self.save_cache()
        with self.lock:
            self.last_refresh = now_iso()
            self.results.get("_errors", {}).pop("worker", None)

    def store_result(self, r):
        name = r["name"]
        if r["ok"]:
            r["at"] = now_iso()
            r["stale_since"] = None
            self.last_good[name] = r
            self.results[name] = r
            return
        prev = self.last_good.get(name)
        if prev:  # 连不上时继续显示上一次的数据，前端标成旧数据
            self.results[name] = {**prev, "ok": False, "error": r["error"],
                                  "need_input": r.get("need_input", False), "warnings": [],
                                  "stale_since": prev["at"]}
        else:
            self.results[name] = {**r, "at": None, "stale_since": None}

    def run_inspect(self, task):
        cluster, dirs = task["cluster"], task["dirs"]
        self._step(task["label"])
        c = self.cluster_by_name(cluster)
        dks = [dir_key(cluster, d) for d in dirs]
        cfg = self.config
        try:
            with self.lock:
                # 每个目录里有哪些 job（给 {id} 模式用，各找各的 slurm 输出），新的在前
                ids_in = {d: [] for d in dirs}
                for jid, j in self._jobs_of(cluster).items():
                    if j.get("workdir") in ids_in:
                        ids_in[j["workdir"]].append(jid)
            plan = [(d, sorted({base_jid(x) for x in ids_in[d]}, key=lambda x: -int(x)
                               if x.isdigit() else 0)[:MAX_IDS_PER_DIR]) for d in dirs]
            results = {}
            blocked = self.auth_blocked.get(cluster)
            if not c:
                err = "集群已经不在 config 里了"
            elif blocked and time.time() < blocked["until"]:
                err = "集群需要 2FA，先在终端登录一次再试"
            else:
                du_timeout = cfg.get("dir_size_timeout_seconds", 20)
                cmd = jobfiles.build_cmd(
                    plan, cfg.get("inspect_files", jobfiles.DEFAULT_PATTERNS),
                    cfg.get("inspect_tail_lines", 30), du_timeout=du_timeout)
                timeout = cfg.get("ssh_timeout_seconds", 15) * 2 + du_timeout * len(dirs)
                try:
                    ok, out, err = run_ssh(c["ssh_host"], cmd, timeout)
                except NeedInput as e:
                    ok, out, err = False, "", f"需要交互输入（2FA/密码）：{e}"
                if ok:
                    parsed = jobfiles.parse_output(out)
                    for n, (d, _) in enumerate(plan):
                        results[d] = parsed.get(n) or {"error": "没拿到输出"}
                    err = None
                else:
                    err = err or "ssh 失败"
            with self.lock:
                for d in dirs:
                    res = results.get(d) or {"error": err}
                    self.inspect_dirs[dir_key(cluster, d)] = {**res, "dir": d, "at": now_iso()}
        finally:
            with self.lock:
                self.inspect_pending.difference_update(dks)
            self._step_done()

    def auto_refresh_loop(self, minutes):
        while True:
            time.sleep(minutes * 60)
            self.enqueue_refresh(force=False)

    def start(self):
        threading.Thread(target=self.worker, daemon=True).start()
        minutes = self.config.get("auto_refresh_minutes", 0) or 0
        if minutes > 0:
            threading.Thread(target=self.auto_refresh_loop, args=(minutes,), daemon=True).start()
        # 默认不自动刷新；但如果一次都没拉过（没有缓存），启动时拉一次
        if self.config.get("refresh_on_start", False) or not self.last_good:
            self.enqueue_refresh(force=True)

    # ---------------- 给前端的数据 ----------------
    def progress(self):
        # 调用方已持有 self.lock
        busy = self.current is not None or bool(self.queue)
        return {
            "busy": busy,
            "label": self.step_label or (self.current or {}).get("label", ""),
            "done": self.batch_done,
            "total": self.batch_total,
            "queued": len(self.queue),
            "refresh_queued": any(t["kind"] == "refresh" for t in self.queue)
                              or (self.current or {}).get("kind") == "refresh",
        }

    def get_status(self):
        with self.lock:
            return {"version": self.version, "progress": self.progress()}

    def get_state(self):
        with self.lock:
            clusters, jobs, errors = [], [], []
            for c in self.clusters():
                name = c["name"]
                r = self.results.get(name)
                if not r:
                    clusters.append({"name": name, "ok": None, "error": None, "qos": [],
                                     "backlog": None, "at": None, "never": True})
                    continue
                if not r["ok"]:
                    errors.append(f"{name}: {r['error']}")
                errors.extend(f"{name}: {w}" for w in r.get("warnings", []))
                stale = bool(r.get("stale_since"))
                for j in r.get("jobs", {}).values():
                    jobs.append({**j, "stale": stale})
                clusters.append({
                    "name": name, "ok": r["ok"], "error": r.get("error"),
                    "need_input": r.get("need_input", False),
                    "stale_since": r.get("stale_since"), "at": r.get("at"),
                    "load_private": r.get("load_private", False), "qos": r.get("qos", []),
                    "backlog": r.get("backlog"),
                })
            errors.extend(self.results.get("_errors", {}).values())
            inspect, pending = self.inspect_view(jobs)
            return json.loads(json.dumps({
                "version": self.version,
                "progress": self.progress(),
                "jobs": jobs,
                "clusters": clusters,
                "updated_at": self.last_refresh,
                "errors": errors,
                "inspect": inspect,
                "inspect_pending": pending,
                "alerts": self.alerts(jobs),
                "settings": {"dir_size_warn_gb": self.config.get("dir_size_warn_gb", 30)},
            }))

    def inspect_view(self, jobs):
        """把按目录存的信息展开成每个 job 一份：只留这个 job 自己的 {id} 文件，
        并注明同目录还有哪些 job。调用方已持有 self.lock。"""
        by_dir = {}
        for j in jobs:
            if j.get("workdir"):
                by_dir.setdefault(dir_key(j["cluster"], j["workdir"]), []).append(j["id"])
        view, pending = {}, []
        for j in jobs:
            key = f"{j['cluster']}:{j['id']}"
            if key in self.inspect_job_err:
                view[key] = self.inspect_job_err[key]
                continue
            if not j.get("workdir"):
                continue
            dk = dir_key(j["cluster"], j["workdir"])
            if dk in self.inspect_pending:
                pending.append(key)
            ins = self.inspect_dirs.get(dk)
            if ins:
                me = base_jid(j["id"])
                view[key] = {**ins,
                             "files": [f for f in ins.get("files", [])
                                       if f.get("for_id", "") in ("", me)],
                             "shared": [x for x in by_dir[dk] if x != j["id"]]}
        return view, pending

    def alerts(self, jobs):
        """界面上的小提醒。调用方已持有 self.lock。"""
        out = []
        # 1) 好几个 job 同时在同一个目录里跑（一般是 dependency 没设对）
        #    同一个 array job 的不同 task 共用目录是正常的，不算
        running = {}
        for j in jobs:
            if j["state"] in ACTIVE and j.get("workdir"):
                running.setdefault((j["cluster"], j["workdir"]), []).append(j)
        for (cluster, d), js in sorted(running.items()):
            if len({base_jid(j["id"]) for j in js}) > 1:
                out.append({"type": "same_dir", "cluster": cluster, "dir": d,
                            "jobs": sorted(j["id"] for j in js),
                            "stale": any(j.get("stale") for j in js)})
        # 2) 目录太大（只对读过的目录有数）
        limit_kb = self.config.get("dir_size_warn_gb", 30) * 1024 * 1024
        by_dir = {}
        for j in jobs:
            if j.get("workdir"):
                by_dir.setdefault(dir_key(j["cluster"], j["workdir"]), []).append(j["id"])
        for dk, ins in sorted(self.inspect_dirs.items()):
            size = ins.get("size_kb")
            if size and size > limit_kb and dk in by_dir:
                cluster, _, d = dk.partition("\u0000")
                out.append({"type": "big_dir", "cluster": cluster, "dir": d, "size_kb": size,
                            "jobs": sorted(by_dir[dk])})
        return out

    def poll_cluster(self, c, force=False):
        cfg = self.config
        name = c["name"]
        now = time.time()
        blocked = self.auth_blocked.get(name)
        if blocked and not force and now < blocked["until"]:
            retry = datetime.fromtimestamp(blocked["until"]).strftime("%H:%M")
            return {"name": name, "ok": False, "need_input": True,
                    "error": f"{blocked['error']}（{retry} 自动重试，或登录后点立即刷新）",
                    "warnings": [], "jobs": {}, "qos": []}

        timeout = cfg.get("ssh_timeout_seconds", 15)
        hours = cfg.get("history_hours", 48)
        with_load = cfg.get("show_cluster_load", True)
        refresh_s = cfg.get("qos_info_refresh_minutes", 30) * 60
        cached = self.qos_info.get(name)
        with_qos_info = cached is None or now - cached["fetched_at"] > refresh_s
        bl_cached = self.backlog.get(name)
        with_backlog = c.get("backlog", cfg.get("backlog", True)) and (
            bl_cached is None
            or now - bl_cached["fetched_at"] > cfg.get("backlog_refresh_minutes", 5) * 60
        )

        steps = [
            ("SQUEUE", f"squeue --me -h -o '{SQUEUE_FORMAT}'"),
            ("SACCT", f"sacct -X --starttime=now-{hours}hours -o {SACCT_FORMAT} "
                      f"--parsable2 --noheader"),
        ]
        if with_load:
            # 全集群（所有用户）每个 QoS 的 running/pending 数，用来判断哪里比较空
            steps.append(("LOAD", "squeue -h -t RUNNING,PENDING -o '%q|%T' | sort | uniq -c"))
        if with_qos_info:
            steps.append(("ASSOC", "sacctmgr -nP show assoc user=$USER format=qos"))
            steps.append(("QOS", f"sacctmgr -nP show qos format={QOS_LIMIT_FORMAT}"))
        if with_backlog:
            steps.append(("ME", "id -un"))
            steps.extend(backlog.COMMANDS)

        timeout_total = timeout * (2 if with_backlog else 1)
        try:
            ok, out, err = run_ssh(c["ssh_host"], build_remote_cmd(steps), timeout_total)
        except NeedInput as e:
            msg = f"需要交互输入（2FA/密码）：{e}。请先在终端里手动 ssh {c['ssh_host']} 登录一次"
            self.auth_blocked[name] = {
                "until": now + cfg.get("auth_retry_minutes", 10) * 60, "error": msg}
            return {"name": name, "ok": False, "need_input": True, "error": msg,
                    "warnings": [], "jobs": {}, "qos": []}
        self.auth_blocked.pop(name, None)
        if not ok:
            return {"name": name, "ok": False, "error": err or "ssh failed", "warnings": [],
                    "jobs": {}, "qos": []}

        sections, failed = split_sections(out)
        warnings = []
        if "END" not in sections or {"SQUEUE", "SACCT"} <= failed:
            return {"name": name, "ok": False, "error": err[-300:] or "squeue/sacct 都失败了",
                    "warnings": [], "jobs": {}, "qos": []}
        for sec in sorted(failed):
            if not sec.startswith("BL_"):  # 积压分析失败会在积压面板里说明，这里不重复报
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

        if with_backlog:
            me = (sections.get("ME") or [""])[0]
            try:
                result, model = backlog.analyze(sections, me)
            except Exception as e:  # noqa: BLE001
                result, model = {"ok": False, "error": f"积压分析出错: {e}"}, None
            result["fetched_at"] = datetime.now(timezone.utc).isoformat()
            bl_cached = {"result": result, "model": model, "fetched_at": now}
            self.backlog[name] = bl_cached
        if bl_cached:
            for j in jobs.values():
                if j["state"] in PENDING and j["source"] == "squeue":
                    j["wait_est"] = backlog.estimate_job(j, bl_cached["model"])

        return {"name": name, "ok": True, "error": None, "warnings": warnings,
                "load_private": load_private,
                "backlog": bl_cached["result"] if bl_cached else None,
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


app = Flask(__name__)
config = load_config()
backend = Backend(config)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/jobs")
def api_jobs():
    return jsonify(backend.get_state())


@app.route("/api/status")
def api_status():
    """很轻：前端每秒问一次，版本号变了才去拉 /api/jobs"""
    return jsonify(backend.get_status())


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    """把一次刷新放进后台队列，马上返回（不等 SSH）"""
    queued = backend.enqueue_refresh(force=True)
    return jsonify({"ok": True, "queued": queued})


@app.route("/api/inspect", methods=["POST"])
def api_inspect():
    """看一批 job 的工作目录（放进后台队列）。body: {"keys": ["cluster:jobid", ...], "force": bool}"""
    body = request.get_json(silent=True) or {}
    added = backend.enqueue_inspect(body.get("keys", []), bool(body.get("force")))
    return jsonify({"ok": True, "added": added})


if __name__ == "__main__":
    backend.start()
    app.run(host=config.get("host", "127.0.0.1"), port=config.get("port", 8787), debug=False,
            threaded=True)
