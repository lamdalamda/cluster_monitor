#!/usr/bin/env python3
"""
HPC Job Dashboard 交互式安装 / Interactive installer
====================================================
    python3 install.py              # 交互式：选 python、生成 config.yaml、（可选）装 systemd 服务
    python3 install.py --uninstall  # 停止并删除 systemd 服务

只用标准库，任何 python3 都能跑。生成 config.yaml 时以 config.example.yaml 为模板
（保留注释），集群列表可以从 ~/.ssh/config 里选；可以顺便测试哪些 Host 上有 Slurm。
装 systemd 服务那一步需要 sudo（会提示输密码）。
"""

import concurrent.futures
import glob
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
from datetime import datetime

APP_DIR = os.path.dirname(os.path.abspath(__file__))
EXAMPLE = os.path.join(APP_DIR, "config.example.yaml")
CONFIG = os.path.join(APP_DIR, "config.yaml")
ASKPASS = os.path.join(APP_DIR, "ssh_askpass_abort.sh")
SERVICE = "hpc-dashboard"
UNIT_PATH = f"/etc/systemd/system/{SERVICE}.service"
DEFAULT_PYTHON = "/home/dx/intel/oneapi/intelpython/latest/envs/pymatgen250508/bin/python"

LANG = "en"
MSG = {
    "lang_q": ("语言 / Language: [1] English  [2] 中文", "Language / 语言: [1] English  [2] 中文"),
    "py_title": ("用哪个 Python 运行 dashboard？", "Which Python should run the dashboard?"),
    "py_choose": ("选编号，或者直接输入路径", "Pick a number or type a path"),
    "py_missing": ("{py} 里缺 flask / pyyaml。", "{py} is missing flask / pyyaml."),
    "py_pip": ("现在用它 pip install -r requirements.txt 吗？", "Run pip install -r requirements.txt with it now?"),
    "py_still": ("还是不行，请手动安装后再运行本脚本。", "Still missing; install them manually and rerun."),
    "py_ok": ("{py}：flask / pyyaml 都有。", "{py}: flask and pyyaml found."),
    "py_bad": ("{py} 不能运行。", "{py} doesn't run."),
    "cfg_exists": ("已经有 config.yaml 了。[k] 保留  [r] 重新生成（旧的会备份）", "config.yaml already exists. [k] keep  [r] regenerate (old one is backed up)"),
    "ssh_read": ("读取 ~/.ssh/config 里的 Host 来选集群吗？", "Read Host entries from ~/.ssh/config to pick clusters?"),
    "ssh_none": ("~/.ssh/config 里没找到具体的 Host。", "No concrete Host entries found in ~/.ssh/config."),
    "probe_q": ("挨个 ssh 上去测试哪些 Host 有 Slurm 吗？（只跑 `command -v sbatch`；需要 2FA 的不会替你输入，直接跳过）",
                "SSH to each Host to test which have Slurm? (runs only `command -v sbatch`; hosts asking for 2FA are skipped, nothing is typed)"),
    "probing": ("正在测试 {n} 个 Host…", "Testing {n} hosts…"),
    "st_slurm": ("Slurm ✓", "Slurm ✓"),
    "st_noslurm": ("没有 Slurm", "no Slurm"),
    "st_2fa": ("需要 2FA/密码", "needs 2FA/password"),
    "st_down": ("连不上", "unreachable"),
    "pick": ("选要监控的集群：编号用逗号/范围（如 1,3,5-7），all = 全部，回车 = 默认 [{d}]",
             "Choose clusters to monitor: numbers, commas, ranges (e.g. 1,3,5-7), all, Enter = default [{d}]"),
    "pick_bad": ("看不懂：{x}", "Can't parse: {x}"),
    "extra": ("还要手动加 ssh 别名吗？（逗号分隔，回车跳过）", "Add more ssh aliases by hand? (comma-separated, Enter to skip)"),
    "no_clusters": ("一个集群都没选，至少要一个。", "No clusters selected; at least one is needed."),
    "names_q": ("显示名用 ssh 别名吗？（选 n 就挨个问）", "Use the ssh alias as display name? (n = ask for each)"),
    "name_for": ("{h} 的显示名", "Display name for {h}"),
    "host_q": ("监听地址：127.0.0.1 = 只有本机（推荐，别的设备用 ssh -L 隧道）；0.0.0.0 = 局域网都能访问（没有登录验证，记得配防火墙）",
               "Listen address: 127.0.0.1 = this machine only (recommended; use an ssh -L tunnel from other devices); 0.0.0.0 = whole network (no authentication, use a firewall)"),
    "port_q": ("端口", "Port"),
    "auto_q": ("自动刷新间隔（分钟，0 = 不自动刷新，只在点按钮时刷新）", "Auto refresh interval (minutes, 0 = only when you click Refresh)"),
    "hist_q": ("sacct 往前查多少小时的历史", "Hours of sacct history to fetch"),
    "size_q": ("目录超过多少 GB 报警", "Warn when a job folder exceeds (GB)"),
    "waitlog_q": ("收集等待时间数据（waitlog/*.csv）吗？", "Collect wait-time data (waitlog/*.csv)?"),
    "preview": ("将写入 config.yaml 的集群：", "Clusters to write to config.yaml:"),
    "write_q": ("写入吗？", "Write it?"),
    "backup": ("旧的 config.yaml 备份为 {p}", "Old config.yaml backed up to {p}"),
    "written": ("已写入 {p}", "Wrote {p}"),
    "abort": ("已取消，什么都没改。", "Cancelled, nothing changed."),
    "keys_warn": ("注意：下面这些 key 设了密码，systemd 服务拿不到 ssh-agent，会连不上：{k}",
                  "Note: these keys have a passphrase; a systemd service can't use your ssh-agent, so it won't connect: {k}"),
    "svc_q": ("装成 systemd 服务（开机自启、挂了自动重启）吗？需要 sudo。", "Install as a systemd service (start on boot, restart on failure)? Needs sudo."),
    "port_busy": ("端口 {p} 现在被占用（可能是手动跑的 python app.py），先停掉它再装服务。继续吗？",
                  "Port {p} is in use (maybe a manually started python app.py); stop it before installing the service. Continue?"),
    "root": ("请用普通用户运行（需要时脚本会自己 sudo），服务要用你的 ssh 配置。", "Run as your normal user (the script sudo's when needed); the service needs your ssh config."),
    "svc_done": ("服务已安装并启动。", "Service installed and started."),
    "svc_fail": ("有一步失败了：{c}", "A step failed: {c}"),
    "unin_done": ("已删除 {s} 服务。", "Removed the {s} service."),
    "done_url": ("网页：http://{h}:{p}", "Open: http://{h}:{p}"),
    "done_logs": ("看日志：journalctl -u {s} -f    改了代码或 config.yaml 之后：sudo systemctl restart {s}",
                  "Logs: journalctl -u {s} -f    After changing code or config.yaml: sudo systemctl restart {s}"),
    "done_manual": ("手动运行：{py} {app}", "Run manually: {py} {app}"),
    "yes_no": (" [Y/n] ", " [Y/n] "),
    "no_yes": (" [y/N] ", " [y/N] "),
}


def m(key, **kw):
    zh, en = MSG[key]
    return (zh if LANG == "zh" else en).format(**kw)


def ask(prompt, default=""):
    suffix = f" [{default}]" if default != "" else ""
    try:
        v = input(f"{prompt}{suffix}: ").strip()
    except EOFError:
        v = ""
    return v or str(default)


def yes(prompt, default=True):
    try:
        v = input(prompt + m("yes_no" if default else "no_yes")).strip().lower()
    except EOFError:
        v = ""
    if not v:
        return default
    return v in ("y", "yes", "是", "好", "1")


def title(s):
    print("\n== " + s)


# ---------------- Python ----------------
def python_ok(py):
    try:
        return subprocess.run([py, "-c", "import flask, yaml"], capture_output=True,
                              timeout=60).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return None


def choose_python():
    title(m("py_title"))
    cands = []
    for p in (DEFAULT_PYTHON, sys.executable, shutil.which("python3")):
        if p and os.path.exists(p) and os.path.realpath(p) not in [os.path.realpath(c) for c in cands]:
            cands.append(p)
    for i, p in enumerate(cands, 1):
        print(f"  [{i}] {p}")
    while True:
        v = ask(m("py_choose"), "1")
        py = cands[int(v) - 1] if v.isdigit() and 1 <= int(v) <= len(cands) else os.path.expanduser(v)
        ok = python_ok(py)
        if ok is None:
            print(m("py_bad", py=py))
            continue
        if not ok:
            print(m("py_missing", py=py))
            if yes(m("py_pip")):
                subprocess.run([py, "-m", "pip", "install", "-r",
                                os.path.join(APP_DIR, "requirements.txt")])
                ok = python_ok(py)
            if not ok:
                print(m("py_still"))
                continue
        print(m("py_ok", py=py))
        return py


# ---------------- ~/.ssh/config ----------------
def parse_ssh_config(path, seen=None):
    """返回具体的 Host 名（跳过通配符），跟着 Include 走"""
    seen = seen if seen is not None else set()
    path = os.path.realpath(os.path.expanduser(path))
    if path in seen or not os.path.isfile(path):
        return []
    seen.add(path)
    hosts = []
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            s = line.split("#", 1)[0].strip()
            if not s:
                continue
            parts = re.split(r"[\s=]+", s, maxsplit=1)
            key, rest = parts[0].lower(), (parts[1] if len(parts) > 1 else "")
            if key == "include":
                for pat in rest.split():
                    pat = os.path.expanduser(pat)
                    if not os.path.isabs(pat):
                        pat = os.path.join(os.path.expanduser("~/.ssh"), pat)
                    for p in sorted(glob.glob(pat)):
                        hosts += parse_ssh_config(p, seen)
            elif key == "host":
                for name in rest.split():
                    if not any(c in name for c in "*?!") and name not in hosts:
                        hosts.append(name)
    return hosts


def ssh_target(alias):
    """ssh -G 解析出来的 user@hostname，只用来显示"""
    try:
        out = subprocess.run(["ssh", "-G", alias], capture_output=True, text=True,
                             timeout=10, stdin=subprocess.DEVNULL).stdout
    except (OSError, subprocess.TimeoutExpired):
        return ""
    kv = dict(line.split(" ", 1) for line in out.splitlines() if " " in line)
    return f"{kv.get('user', '')}@{kv.get('hostname', '')}"


def probe(alias):
    """和 app.py 一样：允许 keyboard-interactive，但一旦要输入就直接杀掉 ssh"""
    env = dict(os.environ, SSH_ASKPASS=ASKPASS, SSH_ASKPASS_REQUIRE="force")
    env.setdefault("DISPLAY", "none")
    try:
        p = subprocess.run(
            ["ssh", "-o", "BatchMode=no", "-o", "KbdInteractiveAuthentication=yes",
             "-o", "ConnectTimeout=8", alias, "command -v sbatch >/dev/null && echo @@SLURM_OK"],
            stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=25, env=env,
            start_new_session=True)
    except (OSError, subprocess.TimeoutExpired):
        return "down"
    if "@@NEED_INPUT" in p.stderr:
        return "2fa"
    if "@@SLURM_OK" in p.stdout:
        return "slurm"
    return "down" if p.returncode == 255 or p.returncode < 0 else "noslurm"


def parse_selection(text, n):
    if text.strip().lower() == "all":
        return list(range(1, n + 1))
    out = []
    for part in re.split(r"[,\s]+", text.strip()):
        if not part:
            continue
        a, _, b = part.partition("-")
        if not a.isdigit() or (b and not b.isdigit()):
            raise ValueError(part)
        lo, hi = int(a), int(b or a)
        if not (1 <= lo <= hi <= n):
            raise ValueError(part)
        out += [i for i in range(lo, hi + 1) if i not in out]
    return out


def existing_cluster_hosts():
    """已有 config.yaml 里的 ssh_host（不依赖 pyyaml，简单地扫一下）"""
    try:
        with open(CONFIG, encoding="utf-8") as f:
            return re.findall(r"^\s*ssh_host:\s*([^\s#]+)", f.read(), re.M)
    except OSError:
        return []


def choose_clusters():
    chosen = []
    if yes(m("ssh_read")):
        hosts = parse_ssh_config("~/.ssh/config")
        if not hosts:
            print(m("ssh_none"))
        else:
            with concurrent.futures.ThreadPoolExecutor(16) as ex:
                targets = dict(zip(hosts, ex.map(ssh_target, hosts)))
            status = {}
            if yes(m("probe_q")):
                print(m("probing", n=len(hosts)))
                with concurrent.futures.ThreadPoolExecutor(16) as ex:
                    status = dict(zip(hosts, ex.map(probe, hosts)))
            before = existing_cluster_hosts()
            for i, h in enumerate(hosts, 1):
                st = m("st_" + status[h]) if h in status else ""
                print(f"  [{i:>2}] {h:<18} {targets.get(h, ''):<40} {st}")
            default = [i for i, h in enumerate(hosts, 1)
                       if status.get(h) == "slurm" or (not status and h in before)
                       or (status.get(h) == "2fa" and h in before)]
            dtext = ",".join(map(str, default))
            while True:
                v = ask(m("pick", d=dtext or "-"), "")
                try:
                    idx = parse_selection(v, len(hosts)) if v else default
                    break
                except ValueError as e:
                    print(m("pick_bad", x=e))
            chosen = [hosts[i - 1] for i in idx]
    extra = ask(m("extra"), "")
    chosen += [h for h in re.split(r"[,\s]+", extra) if h and h not in chosen]
    if not chosen:
        print(m("no_clusters"))
        return choose_clusters()
    names = {h: h for h in chosen}
    if not yes(m("names_q")):
        for h in chosen:
            names[h] = ask(m("name_for", h=h), h)
    return [(names[h], h) for h in chosen]


# ---------------- config.yaml ----------------
def yaml_scalar(v):
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    return v if re.fullmatch(r"[A-Za-z0-9_./-]+", v) else json.dumps(v, ensure_ascii=False)


def render_config(template, clusters, values):
    """以 config.example.yaml 为模板：替换 clusters 列表和几个顶层值，注释原样保留"""
    lines = template.splitlines()
    start = next(i for i, l in enumerate(lines) if l.startswith("clusters:"))
    end = next(i for i in range(start + 1, len(lines)) if lines[i] and not lines[i][0].isspace())
    block = ["clusters:"]
    for k, (name, host) in enumerate(clusters):
        block += [f"  - name: {yaml_scalar(name)}", f"    ssh_host: {yaml_scalar(host)}", "    enabled: true"]
        if k == 0:
            block += ["    # qos:            # 可选 / optional", "    #   - freecpu",
                      "    #   - name: freegpu", "    #     note: GPU, max 2 days"]
        block.append("")
    out = "\n".join(lines[:start] + block + lines[end:]) + "\n"
    for key, val in values.items():
        out, n = re.subn(rf"^{key}:[^\n#]*", f"{key}: {yaml_scalar(val)}", out, count=1, flags=re.M)
        if not n:
            out += f"{key}: {yaml_scalar(val)}\n"
    return out


def ask_int(prompt, default):
    while True:
        v = ask(prompt, default)
        try:
            return int(v) if float(v).is_integer() else float(v)
        except ValueError:
            pass


def make_config():
    clusters = choose_clusters()
    values = {
        "host": ask(m("host_q"), "127.0.0.1"),
        "port": ask_int(m("port_q"), 8787),
        "auto_refresh_minutes": ask_int(m("auto_q"), 0),
        "history_hours": ask_int(m("hist_q"), 48),
        "dir_size_warn_gb": ask_int(m("size_q"), 30),
        "waitlog": yes(m("waitlog_q")),
    }
    title(m("preview"))
    for name, host in clusters:
        print(f"  - {name} ({host})")
    for k, v in values.items():
        print(f"  {k}: {yaml_scalar(v)}")
    if not yes(m("write_q")):
        print(m("abort"))
        sys.exit(1)
    with open(EXAMPLE, encoding="utf-8") as f:
        text = render_config(f.read(), clusters, values)
    if os.path.exists(CONFIG):
        bak = f"{CONFIG}.bak.{datetime.now():%Y%m%d%H%M%S}"
        shutil.copy2(CONFIG, bak)
        print(m("backup", p=bak))
    with open(CONFIG, "w", encoding="utf-8") as f:
        f.write(text)
    print(m("written", p=CONFIG))
    return values, [h for _, h in clusters]


def read_values():
    vals = {"host": "127.0.0.1", "port": 8787}
    try:
        with open(CONFIG, encoding="utf-8") as f:
            text = f.read()
        for k in vals:
            mm = re.search(rf"^{k}:\s*([^\s#]+)", text, re.M)
            if mm:
                vals[k] = mm.group(1).strip("\"'")
    except OSError:
        pass
    return vals


# ---------------- systemd ----------------
def passphrase_keys(hosts):
    keys = set()
    for h in hosts:
        try:
            out = subprocess.run(["ssh", "-G", h], capture_output=True, text=True, timeout=10).stdout
        except (OSError, subprocess.TimeoutExpired):
            continue
        for line in out.splitlines():
            if line.startswith("identityfile "):
                p = os.path.expanduser(line.split(" ", 1)[1])
                if os.path.isfile(p) and subprocess.run(
                        ["ssh-keygen", "-y", "-P", "", "-f", p], capture_output=True).returncode:
                    keys.add(p)
    return sorted(keys)


def port_busy(port):
    with socket.socket() as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("0.0.0.0", int(port)))
            return False
        except OSError:
            return True


def run_sudo(cmds):
    for c in cmds:
        print("  $ sudo " + " ".join(c))
        if subprocess.run(["sudo"] + c).returncode != 0:
            print(m("svc_fail", c=" ".join(c)))
            return False
    return True


def install_service(py, port):
    if port_busy(port) and not yes(m("port_busy", p=port), default=False):
        return False
    user = os.environ.get("USER") or os.getlogin()
    unit = f"""[Unit]
Description=HPC Job Dashboard ({APP_DIR})
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User={user}
WorkingDirectory={APP_DIR}
ExecStart={py} {os.path.join(APP_DIR, "app.py")}
Environment=PYTHONUNBUFFERED=1
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
"""
    with tempfile.NamedTemporaryFile("w", suffix=".service", delete=False) as f:
        f.write(unit)
        tmp = f.name
    try:
        ok = run_sudo([
            ["install", "-m", "644", tmp, UNIT_PATH],
            ["systemctl", "daemon-reload"],
            ["systemctl", "enable", SERVICE],
            ["systemctl", "restart", SERVICE],
        ])
    finally:
        os.unlink(tmp)
    if ok:
        subprocess.run(["systemctl", "--no-pager", "--lines=5", "status", SERVICE])
        print(m("svc_done"))
    return ok


def uninstall():
    run_sudo([["systemctl", "disable", "--now", SERVICE]])
    if run_sudo([["rm", "-f", UNIT_PATH], ["systemctl", "daemon-reload"]]):
        print(m("unin_done", s=SERVICE))


def main():
    global LANG
    LANG = "zh" if os.environ.get("LANG", "").lower().startswith("zh") else "en"
    if os.geteuid() == 0:
        print(m("root"))
        sys.exit(1)
    if "--uninstall" in sys.argv:
        uninstall()
        return
    v = ask(m("lang_q"), "2" if LANG == "zh" else "1")
    LANG = "zh" if v in ("2", "zh", "中文") else "en"
    os.chmod(ASKPASS, 0o755)

    py = choose_python()

    hosts = existing_cluster_hosts()
    if os.path.exists(CONFIG) and ask(m("cfg_exists"), "k").lower().startswith("k"):
        values = read_values()
    else:
        values, hosts = make_config()

    title("systemd")
    if yes(m("svc_q"), default=False):
        bad = passphrase_keys(hosts)
        if bad:
            print(m("keys_warn", k=", ".join(bad)))
        install_service(py, values["port"])
    else:
        print(m("done_manual", py=py, app=os.path.join(APP_DIR, "app.py")))
    host = "127.0.0.1" if values["host"] in ("0.0.0.0", "::") else values["host"]
    print(m("done_url", h=host, p=values["port"]))
    print(m("done_logs", s=SERVICE))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print()
        sys.exit(130)
