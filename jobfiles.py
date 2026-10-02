"""
看 job 工作目录里有什么
======================
展开一组 job 时，对每个 job 的工作目录（squeue %Z / sacct WorkDir）跑一次；
多个 job 共用同一个目录时只读一次：
- ls -lt：目录里有哪些文件、什么时候改的；du -sk：目录总大小（带超时）
- 对 config.yaml 里 inspect_files 的每个模式（默认 vasp.out、Slab*.log、*{id}*.out*），
  取最新的那个匹配文件：tail 最后几行 + grep 一下关键行（离子步、收敛、报错）。
  带 {id} 的模式对这个目录里的每个 job 各找一次（每个 job 自己的 slurm 输出）。
然后在本地解析成摘要（离子步数、能量、是否收敛、最后一条报错）。
"""

import re
import shlex

DEFAULT_PATTERNS = ["vasp.out", "Slab*.log", "*{id}*.out*"]

# grep 关键行：VASP 离子步 / 收敛 / 常见报错，以及通用的 error / traceback
GREP_RE = (
    r"F=|reached required accuracy|VERY BAD NEWS|ZBRENT: fatal|EDDDAV|BRMIX|"
    r"[Ee]rror|ERROR|Traceback|BAD TERMINATION|[Kk]illed|TIME LIMIT|CANCELLED"
)
IONIC_RE = re.compile(
    r"^\s*(\d+)\s+F=\s*([-+.\dEe]+)\s+E0=\s*([-+.\dEe]+)\s+d E\s*=\s*([-+.\dEe]+)"
    r"(?:\s+mag=\s*([-+.\dEe]+))?"
)
ELEC_RE = re.compile(r"^\s*(DAV|RMM|CG|SDA|NONE):\s*(\d+)\s+([-+.\dEe]+)\s+([-+.\dEe]+)")
ERROR_RE = re.compile(
    r"VERY BAD NEWS|ZBRENT: fatal|EDDDAV|BRMIX: very serious|[Ee]rror|ERROR|Traceback|"
    r"BAD TERMINATION|[Kk]illed|DUE TO TIME LIMIT|CANCELLED"
)
LS_RE = re.compile(r"^([-dlcbps])\S*\s+\d+\s+\S+\s+\S+\s+(\d+)\s+(\S+)\s+(.+)$")


def build_cmd(dirs, patterns, tail_lines=30, ls_lines=40, du_timeout=20):
    """dirs: [(workdir, [jobid, ...])]。每个目录放在子 shell 里，cd 失败不影响后面的。
    输出里目录用序号 @@DIR <n> 标记（不用路径，免得路径里有奇怪字符）。"""

    def one_file(pat, tag):
        return (
            f"f=$(ls -td -- {pat} 2>/dev/null | head -n 1); "
            f"if [ -n \"$f\" ] && [ -f \"$f\" ]; then echo \"@@FILE {tag}|$f\"; "
            f"tail -n {int(tail_lines)} -- \"$f\"; echo @@GREP; "
            f"grep -aE '{GREP_RE}' -- \"$f\" | tail -n 8; fi"
        )

    parts = []
    for n, (workdir, ids) in enumerate(dirs):
        file_cmds = []
        for pat in patterns:
            # 模式来自你自己的 config，不转义，允许通配符；{id} 换成每个 job 的 id
            if "{id}" in pat:
                for jid in ids:
                    clean = re.sub(r"[^\w]", "", jid.split("_")[0])
                    file_cmds.append(one_file(pat.replace("{id}", clean), clean))
            else:
                file_cmds.append(one_file(pat, ""))
        du = (f"echo \"@@DU $(timeout {int(du_timeout)} du -sk . 2>/dev/null | cut -f1)\"; "
              if du_timeout else "")
        parts.append(
            f"echo '@@DIR {n}'; ( if cd -- {shlex.quote(workdir)} 2>/dev/null; then "
            f"echo \"@@COUNT $(ls -1A | wc -l)\"; {du}echo @@LS; "
            f"ls -lt --time-style=+%m-%d_%H:%M 2>/dev/null | head -n {int(ls_lines) + 1}; "
            + "; ".join(file_cmds)
            + "; else echo @@NODIR; fi )"
        )
    parts.append("echo @@END")
    return "; ".join(parts)


def _num(s):
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def parse_file(name, tail, grep):
    """从 tail / grep 的行里解析出摘要"""
    info = {}
    for line in grep + tail:  # grep 覆盖整个文件，tail 里可能有更新的一行
        m = IONIC_RE.match(line)
        if m:
            step = int(m.group(1))
            if step >= info.get("ionic_step", -1):
                info.update(ionic_step=step, F=_num(m.group(2)), E0=_num(m.group(3)),
                            dE=_num(m.group(4)))
                if m.group(5):
                    info["mag"] = _num(m.group(5))
    for line in reversed(tail):
        m = ELEC_RE.match(line)
        if m:
            info.update(elec_algo=m.group(1), elec_step=int(m.group(2)),
                        elec_E=_num(m.group(3)), elec_dE=_num(m.group(4)))
            break
    if any("reached required accuracy" in line for line in grep + tail):
        info["converged"] = True
    for line in reversed(tail + grep):
        if ERROR_RE.search(line) and "ZBRENT: bracketing" not in line:
            info["error"] = line.strip()[:200]
            break
    last = next((line.strip() for line in reversed(tail) if line.strip()), "")
    info["last_line"] = last[:200]
    return info


def parse_output(output):
    """-> {目录序号: {...}}"""
    results, cur, section, cur_file = {}, None, None, None
    for raw in output.splitlines():
        line = raw.rstrip("\n")
        s = line.strip()
        if s.startswith("@@DIR "):
            cur = {"exists": True, "count": None, "size_kb": None, "ls": [], "files": []}
            try:
                results[int(s[6:])] = cur
            except ValueError:
                cur = None
            section, cur_file = None, None
            continue
        if cur is None:
            continue  # 登录脚本打印的东西
        if s == "@@END":
            break
        if s == "@@NODIR":
            cur["exists"] = False
            continue
        if s.startswith("@@DU"):
            v = s[4:].strip()
            cur["size_kb"] = int(v) if v.isdigit() else None  # 空 = du 超时
            continue
        if s.startswith("@@COUNT "):
            try:
                cur["count"] = int(s[8:])
            except ValueError:
                pass
            continue
        if s == "@@LS":
            section = "ls"
            continue
        if s.startswith("@@FILE "):
            tag, _, fname = s[7:].partition("|")
            cur_file = {"name": fname.strip(), "for_id": tag.strip(), "tail": [], "grep": []}
            cur["files"].append(cur_file)
            section = "tail"
            continue
        if s == "@@GREP":
            section = "grep"
            continue
        if section == "ls":
            m = LS_RE.match(s)
            if m:
                cur["ls"].append({"name": m.group(4), "is_dir": m.group(1) == "d",
                                  "size": int(m.group(2)), "mtime": m.group(3).replace("_", " ")})
        elif section in ("tail", "grep") and cur_file is not None:
            cur_file[section].append(line[:300])

    for r in results.values():
        mtimes = {e["name"]: e["mtime"] for e in r["ls"]}
        for f in r["files"]:
            f["mtime"] = mtimes.get(f["name"], "")
            f["parsed"] = parse_file(f["name"], f["tail"], f["grep"])
            del f["grep"]
    return results
