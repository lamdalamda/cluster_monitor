"""
纯文本视图（给 Claude / 命令行用）
=================================
和网页用的是同一份数据（Backend.get_state()），只是输出成对齐的纯文本，方便 curl 之后直接读。
默认单独监听 text_port（8786），网页那个端口下面也挂了一份：/text/...

  GET  /                 总览：各集群、提醒、上次刷新以来结束的 job、在跑的 job
  GET  /jobs             job 列表（默认按名字分组；?flat=1 一行一个；?state= ?cluster= ?q= ?limit=）
  GET  /job/<集群>/<id>   一个 job 的全部信息 + 工作目录（?read=1 先去读目录，最多等 90 秒）
  GET  /done             上次刷新以来结束的 job
  GET  /alerts           提醒
  GET  /qos              QoS → 分区：我的 / 全集群 / 节点 / 预计等待 / 限额（?cluster=）
  GET  /fairshare        fairshare（sshare）、优先级各项（sprio）、排队的 job 前面有多少、前面是谁（?cluster=）
  GET  /suggest          新 job 交到哪里最快（?cpus=64&gpus=0&nodes=1&time_h=24&cluster=）
  POST /refresh          立即刷新（默认等刷新完再返回总览；?wait=0 不等）
  GET  /help             这份说明
"""

import os
import time
from datetime import datetime

from flask import Blueprint, Response, request

from schedulers.base import ACTIVE, PENDING, sort_jobid

BAD = {"FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL", "OUT_OF_MEMORY", "PREEMPTED", "BOOT_FAIL", "DEADLINE"}
SHORT = {"RUNNING": "R", "COMPLETING": "CG", "CONFIGURING": "CF", "PENDING": "PD", "DEPEND": "DEP",
         "SUSPENDED": "S", "REQUEUED": "RQ", "COMPLETED": "CD", "CANCELLED": "CA", "FAILED": "F",
         "TIMEOUT": "TO", "NODE_FAIL": "NF", "OUT_OF_MEMORY": "OOM", "PREEMPTED": "PR"}
HELP = """endpoints (plain text; same data as the web page):
  GET  /                 overview: clusters, alerts, jobs finished since last refresh, running jobs
  GET  /jobs             jobs grouped by name (?flat=1 one per line; ?state=active|running|pending|dep|done|failed|just_done|all
                         ?cluster=NAME ?q=TEXT ?limit=N)
  GET  /job/CLUSTER/ID   everything about one job + its working folder (?read=1 reads the folder first, waits <= 90 s)
  GET  /done             jobs that finished between the last two refreshes (success or not)
  GET  /alerts           alerts (never-run reasons, jobs sharing a folder, big folders, slow VASP loops)
  GET  /qos              QoS -> partition tree: mine / cluster / nodes / expected wait / limits (?cluster=NAME)
  GET  /fairshare        fairshare (sshare), priority factors (sprio), how many queued jobs are ahead of mine and whose
                         (?cluster=NAME)
  GET  /suggest          where a new job would start soonest (?cpus=64&gpus=0&nodes=1&time_h=24&cluster=NAME)
  POST /refresh          refresh now; waits for it to finish and returns the overview (?wait=0 to return at once)
notes: job times are cluster-local; "dep" = pending but waiting on a dependency (not really queueing);
  counts like 4 (+8) / 410 (+384) = running (+deps whose chain head is running) / pending (+deps whose head is queued);
  expected wait = model median [q20-q80], "<= X" = backlog estimate, an upper bound in practice;
  "#N/M @part" = queue position among eligible pending jobs in that partition (deps / held not counted);
  dir = working folder (for groups: the shown job's folder, "+N dirs" if the group spans several)."""


def en(x):
    """{"zh": ..., "en": ...} -> 英文"""
    if isinstance(x, dict):
        return x.get("en") or x.get("zh") or ""
    return "" if x is None else str(x)


def fmt_h(h):
    if h is None:
        return "-"
    return "0h" if h < 0.1 else f"{h / 24:.1f}d" if h > 48 else f"{h:.1f}h"


def fmt_size(n):
    for unit in ("B", "K", "M", "G"):
        if n < 1024 or unit == "G":
            return f"{n:.0f}{unit}" if unit in ("B", "K") else f"{n:.1f}{unit}"
        n /= 1024


def ct(s):
    """集群本地时间 2026-10-28T16:31:07 -> 10-28 16:31"""
    return s[5:16].replace("T", " ") if s and len(s) >= 16 else (s or "-")


def lt(iso):
    """UTC ISO -> 本机时间 11-03 14:05"""
    if not iso:
        return "-"
    try:
        return datetime.fromisoformat(iso).astimezone().strftime("%m-%d %H:%M")
    except ValueError:
        return iso


def ago(iso):
    try:
        m = (time.time() - datetime.fromisoformat(iso).timestamp()) / 60
    except (TypeError, ValueError):
        return ""
    return f"{m:.0f} min ago" if m < 120 else f"{m / 60:.1f} h ago"


def table(rows, header=None):
    """对齐的列；最后一列不补空格"""
    rows = [[str(c) for c in r] for r in ([header] if header else []) + rows]
    if not rows:
        return ""
    w = [max(len(r[i]) for r in rows if i < len(r)) for i in range(max(map(len, rows)))]
    return "\n".join("  ".join(c.ljust(w[i]) if i < len(r) - 1 else c for i, c in enumerate(r)).rstrip()
                     for r in rows)


def disp_state(j):
    return "DEPEND" if j["state"] in PENDING and j.get("dep_wait") else j["state"]


def counts_text(r, m="mine"):
    """4 (+8) / 410 (+384)"""
    run, pd = r.get(f"{m}_running"), r.get(f"{m}_pending")
    if run is None or pd is None:
        return "-"
    dep, dep_run = r.get(f"{m}_dep") or 0, r.get(f"{m}_dep_run") or 0
    left = f"{run} (+{dep_run})" if dep_run else f"{run}"
    right = f"{pd - dep} (+{dep - dep_run})" if dep - dep_run else f"{pd - dep}"
    return f"{left} / {right}"


def when_text(j):
    """跑着的：开始时间；排队的：Slurm 估计 / 模型 / 积压；结束的：结束时间"""
    if j["state"] in ACTIVE:
        return f"started {ct(j.get('start'))}"
    if j["state"] not in PENDING:
        return f"ended {ct(j.get('end'))}"
    if j.get("never_run"):
        return f"WILL NOT START ({j['never_run']})"
    bits = []
    if j.get("start"):
        bits.append(f"slurm est {ct(j['start'])}")
    m = j.get("ml_est")
    if m:
        few = " (little data)" if m.get("n_seen", 0) < 3 else ""
        if m.get("start"):
            bits.append(f"model {ct(m['start'][1])} [{ct(m['start'][0])} - {ct(m['start'][2])}]{few}")
        else:
            bits.append(f"model: {fmt_h(m['wait'][1])} after deps finish{few}")
    w = j.get("wait_est")
    if w and w.get("bottleneck"):
        bits.append(f"backlog <= {fmt_h(w['bottleneck']['max'])}")
    return "; ".join(bits) or "-"


def prio_text(j):
    """#1921/1941 @cpu：在这个分区排第几（多个候选分区时取最靠前的那个）"""
    p = j.get("prio")
    if not p:
        return ""
    part, x = min(p["parts"].items(), key=lambda kv: kv[1]["ahead"]) if p.get("parts") else ("", None)
    return f"#{x['ahead'] + 1}/{x['competing'] + 1} @{part}" if x and x["competing"] else ""


def where_text(j):
    if j["state"] in ACTIVE:
        return j.get("nodelist") or "-"
    if j["state"] in PENDING:
        return "; ".join(x for x in (j.get("reason") or "-", prio_text(j)) if x)
    exit_code = j.get("exit_code")
    return f"exit {exit_code}" if exit_code else "-"


def job_line(j):
    return [j["cluster"], j["id"], SHORT.get(disp_state(j), disp_state(j)), j.get("name", ""),
            j.get("qos") or "-", j.get("partition") or "-",
            f"{j.get('time_used') or '-'}/{j.get('time_limit') or '-'}", where_text(j), when_text(j), j.get("workdir") or "-"]


JOB_HEADER = ["cluster", "id", "st", "name", "qos", "partition", "used/limit", "nodes/reason/exit", "when", "dir"]


def dirs_text(js, rep):
    """一组 job 的工作目录：显示代表 job 的，其他目录不一样时注明一共几个"""
    dirs = {j.get("workdir") for j in js if j.get("workdir")}
    d = rep.get("workdir") or (min(dirs) if dirs else "-")
    return d + (f"  (+{len(dirs) - 1} dirs under {os.path.commonpath(sorted(dirs))})" if len(dirs) > 1 else "")


def match_state(j, state, just_done):
    s = j["state"]
    return {
        "all": True,
        "active": s in ACTIVE | PENDING,
        "running": s in ACTIVE,
        "pending": s in PENDING and not j.get("dep_wait"),
        "dep": s in PENDING and bool(j.get("dep_wait")),
        "done": s not in ACTIVE | PENDING,
        "failed": s in BAD,
        "just_done": (j["cluster"], j["id"]) in just_done,
    }.get(state, True)


def just_done_set(state):
    out = set()
    for c in state["clusters"]:
        for jid in (c.get("just_done") or {}).get("ids", []):
            out.add((c["name"], jid))
    return out


def filter_jobs(state, args):
    st = args.get("state", "active")
    jd = just_done_set(state)
    q = (args.get("q") or "").lower()
    out = []
    for j in state["jobs"]:
        if args.get("cluster") and j["cluster"] != args["cluster"]:
            continue
        if not match_state(j, st, jd):
            continue
        if q and q not in " ".join(str(j.get(k) or "") for k in ("id", "name", "nodelist", "qos", "partition", "workdir")).lower():
            continue
        out.append(j)
    return out


# ---------------- 各部分 ----------------
def header_text(state):
    a = state.get("auto_refresh") or {}
    auto = f"auto refresh every {a['minutes'] / 60:g}h, next {lt(a.get('next'))}" if a.get("minutes") else "auto refresh off"
    upd = state.get("updated_at")
    lines = [f"HPC jobs @ {datetime.now().strftime('%Y-%m-%d %H:%M')}  "
             f"(last refresh {lt(upd)}, {ago(upd)}; {auto})" if upd else "HPC jobs: never refreshed"]
    p = state.get("progress") or {}
    if p.get("busy"):
        lines.append(f"[busy: {en(p.get('label'))} {p.get('done', 0)}/{p.get('total', 0)}]")
    return "\n".join(lines)


def clusters_text(state):
    rows = []
    for c in state["clusters"]:
        if c.get("never"):
            rows.append([c["name"], "never fetched", "", ""])
            continue
        qos = c.get("qos") or []
        tot = {k: sum(q.get(k) or 0 for q in qos) for k in
               ("mine_running", "mine_pending", "mine_dep", "mine_dep_run")}
        status = "ok" if c["ok"] else (f"STALE since {lt(c['stale_since'])}: {en(c.get('error'))}"
                                       if c.get("stale_since") else f"ERROR: {en(c.get('error'))}")
        rows.append([c["name"], status, f"data {lt(c.get('at'))}", f"mine run (+dep) / pend (+dep): {counts_text(tot)}"])
    out = table(rows)
    errs = [en(e) for e in state.get("errors") or []]
    shown = {f"{c['name']}: {en(c.get('error'))}" for c in state["clusters"] if not c.get("ok")}
    errs = [e for e in errs if e not in shown]
    if errs:
        out += "\nerrors:\n" + "\n".join(f"  {e}" for e in errs)
    return out


def alerts_text(state):
    al = state.get("alerts") or []
    if not al:
        return "no alerts"
    lines = []
    for a in al:
        jobs = " ".join(a.get("jobs", [])[:20]) + (f" ... ({len(a['jobs'])} jobs)" if len(a.get("jobs", [])) > 20 else "")
        t = a["type"]
        if t == "never_run":
            lines.append(f"- NEVER RUN  {a['cluster']}  reason {a['reason']}: {jobs}")
        elif t == "same_dir":
            lines.append(f"- SAME FOLDER  {a['cluster']}  {len(a['jobs'])} jobs running in {a['dir']}: {jobs}")
        elif t == "big_dir":
            lines.append(f"- BIG FOLDER  {a['cluster']}  {fmt_size(a['size_kb'] * 1024)} {a['dir']}: {jobs}")
        elif t == "many_files":
            lines.append(f"- MANY FILES  {a['cluster']}  {a.get('count')} entries {a['dir']}: {jobs}")
        elif t == "slow_loop":
            lines.append(f"- SLOW LOOP  {a['cluster']}  {en(a.get('reason'))} {a['dir']}: {jobs}")
        elif t == "waitlog_full":
            lines.append(f"- WAITLOG FULL  {a.get('size_mb')} MB > {a.get('limit_mb')} MB, logging stopped: {a.get('dir')}")
        else:
            lines.append(f"- {t}  {a.get('cluster', '')} {a.get('dir', '')} {jobs}")
    return "\n".join(lines)


def done_text(state, limit=None):
    by_key = {(j["cluster"], j["id"]): j for j in state["jobs"]}
    jobs, spans = [], []
    for c in state["clusters"]:
        jd = c.get("just_done")
        if not jd:
            continue
        spans.append(f"{c['name']} {lt(jd['since'])} -> {lt(jd['until'])}")
        jobs += [by_key[(c["name"], i)] for i in jd["ids"] if (c["name"], i) in by_key]
        jobs += jd.get("gone", [])
    if not spans:
        return "no comparison yet (needs two successful refreshes)"
    # 出问题的在前，然后按结束时间新的在前
    bad = [j for j in jobs if j["state"] in BAD or j.get("gone")]
    good = [j for j in jobs if j not in bad]
    good.sort(key=lambda j: j.get("end") or "", reverse=True)
    jobs = bad + good
    counts = {}
    for j in jobs:
        k = "VANISHED" if j.get("gone") else j["state"]
        counts[k] = counts.get(k, 0) + 1
    head = (f"{len(jobs)} finished ({', '.join(f'{k} {v}' for k, v in sorted(counts.items()))})" if jobs else "none finished") \
        + "; windows: " + "; ".join(spans)
    if not jobs:
        return head
    rows = [[j["cluster"], j["id"], "VANISHED" if j.get("gone") else j["state"], j.get("name", ""),
             "" if j.get("gone") else f"exit {j.get('exit_code') or '-'}",
             "" if j.get("gone") else f"ran {j.get('time_used') or '-'}/{j.get('time_limit') or '-'}",
             "" if j.get("gone") else f"ended {ct(j.get('end'))}",
             j.get("workdir") or ""] for j in jobs[:limit]]
    more = f"\n... {len(jobs) - limit} more (GET /done)" if limit and len(jobs) > limit else ""
    return head + "\n" + table(rows) + more + (
        "\nVANISHED = was running/pending last time, now in neither queue nor history" if any(j.get("gone") for j in jobs) else "")


def groups_text(jobs, limit=None):
    """同集群同名字的收成一行"""
    groups = {}
    for j in jobs:
        groups.setdefault((j["cluster"], j.get("name", "")), []).append(j)
    rows = []

    def rank(js):
        return 0 if any(j["state"] in ACTIVE for j in js) else 1 if any(j["state"] in PENDING for j in js) else 2
    items = sorted(groups.items(), key=lambda kv: (rank(kv[1]), kv[0][0], -max(sort_jobid(j["id"]) for j in kv[1])))
    for (cluster, name), js in items[:limit]:
        js.sort(key=lambda j: sort_jobid(j["id"]))
        rep = next((j for j in js if j["state"] in ACTIVE), None) \
            or next((j for j in js if j["state"] in PENDING and not j.get("dep_wait")), None) \
            or next((j for j in js if j["state"] in PENDING), None) or js[-1]
        cnt = {}
        for j in js:
            s = SHORT.get(disp_state(j), disp_state(j))
            cnt[s] = cnt.get(s, 0) + 1
        ids = js[0]["id"] if len(js) == 1 else f"{js[0]['id']}..{js[-1]['id']}"
        rows.append([cluster, name, f"x{len(js)}", " ".join(f"{k}{v}" for k, v in cnt.items()), ids,
                     f"now {rep['id']} {SHORT.get(disp_state(rep), disp_state(rep))}", where_text(rep), when_text(rep),
                     dirs_text(js, rep)])
    more = f"\n... {len(items) - limit} more groups" if limit and len(items) > limit else ""
    return table(rows, ["cluster", "name", "n", "states", "ids", "current", "nodes/reason/exit", "when", "dir"]) + more


def overview_text(state):
    running = [j for j in state["jobs"] if j["state"] in ACTIVE]
    parts = [header_text(state), "", "## clusters  (counts: mine running (+deps behind running) / pending (+deps behind queued))",
             clusters_text(state), "", "## alerts", alerts_text(state), "",
             "## finished since last refresh", done_text(state, limit=25), "",
             "## fairshare / priority  (details: GET /fairshare)", fairshare_text(state), "",
             f"## running ({len(running)})", table([job_line(j) for j in sorted(running, key=lambda j: (j['cluster'], sort_jobid(j['id'])))], JOB_HEADER) or "none",
             "", "## queued job groups (pending / waiting on deps, grouped by name)",
             groups_text([j for j in state["jobs"] if j["state"] in PENDING and not any(
                 k["state"] in ACTIVE and k["cluster"] == j["cluster"] and k.get("name") == j.get("name") for k in running)], limit=40),
             "", HELP]
    return "\n".join(parts)


def qos_text(state, cluster=None):
    out = []
    for c in state["clusters"]:
        if cluster and c["name"] != cluster or c.get("never"):
            continue
        ov = c.get("overview") or {}
        parts = ov.get("partitions") or {}
        out.append(f"## {c['name']}  ({c.get('qos_label') or 'QoS'}; data {lt(c.get('at'))}"
                   + ("" if c.get("ok") else f"; STALE: {en(c.get('error'))}") + ")")
        qrow = {q["name"]: q for q in c.get("qos") or []}
        rows = []
        for g in ov.get("groups") or [{"qos": q["name"], "children": []} for q in c.get("qos") or []]:
            q = qrow.get(g["qos"], {"name": g["qos"]})
            lim = " ".join(x for x in (f"MaxJobs={q['max_jobs_pu']}" if q.get("max_jobs_pu") else "",
                                       f"MaxSubmit={q['max_submit_pu']}" if q.get("max_submit_pu") else "",
                                       q.get("max_tres_pu") or "") if x)
            rows.append([q["name"], counts_text(q), "hidden" if c.get("load_private") else counts_text(q, "all"),
                         "", "", "", f"maxwall {q.get('max_wall') or '-'}", lim])
            for ch in g.get("children", []):
                if ch.get("hidden"):
                    continue
                p = parts.get(ch["name"], {})
                use = lambda x: "-" if not x else (f"{x['total']}" if "idle" not in x else f"{x['alloc']}/{x['idle']}/{x['total']}")  # noqa: E731
                m, b = ch.get("ml"), p.get("backlog")
                wait = ""
                if m:
                    wait = f"~{fmt_h(m['wait'][1])} [{fmt_h(m['wait'][0])}-{fmt_h(m['wait'][2])}]" + (" (little data)" if m.get("n_seen", 0) < 3 else "")
                if b:
                    wait += f" <= {fmt_h(b['cpu_max'])}"
                pq = p.get("part_qos_limits") or {}
                plim = (f"{p['part_qos']}: " + " ".join(x for x in (f"MaxJobs={pq.get('max_jobs_pu')}" if pq.get("max_jobs_pu") else "",
                                                                   f"MaxSubmit={pq.get('max_submit_pu')}" if pq.get("max_submit_pu") else "",
                                                                   pq.get("max_tres_pu") or "") if x)) if p.get("part_qos") else ""
                rows.append(["  " + ch["name"] + (" (default)" if p.get("default") else "") + ("" if (p.get("avail") or "up") == "up" else f" [{p['avail']}]"),
                             counts_text(ch), "hidden" if c.get("load_private") else counts_text(ch, "all"),
                             use(p.get("nodes")), use(p.get("cpus")), wait.strip() or "-", f"limit {p.get('time_limit') or '-'}", plim])
        out.append(table(rows, ["qos / partition", "mine r/p", "cluster r/p", "nodes a/i/t", "cpus a/i/t", "expected wait", "time", "limits"]))
        if ov.get("other"):
            out.append(f"other partitions (hidden: no jobs or hide_partitions): {', '.join(ov['other'])}")
        out.append("")
    return "\n".join(out).rstrip() or "no such cluster"


def kfmt(n):
    """1000000 -> 1M, 25300 -> 25k"""
    for d, u in ((1e6, "M"), (1e3, "k")):
        if abs(n) >= d:
            return f"{n / d:.3g}{u}"
    return f"{n:g}"


def share_text(a):
    """用了自己份额的几倍：LevelFS = 份额 / 实际用量"""
    lvl = a.get("level_fs")
    if a.get("eff_usage") == 0:
        return "unused"
    return f"used {1 / lvl:.2g}x your share" if lvl else "share ?"


def fairshare_text(state, cluster=None, detail=False):
    """每个集群几行：fairshare 和别人比怎么样、优先级主要看什么、排第几"""
    out = []
    for c in state["clusters"]:
        fs = c.get("fairshare")
        if cluster and c["name"] != cluster or c.get("never"):
            continue
        if not fs:
            out.append(f"{c['name']}: no fairshare data" + ("" if "fairshare" in (c.get("features") or [])
                                                             else f" ({c.get('scheduler')} not supported)"))
            continue
        name = c["name"] + ("" if c.get("ok") else f" [stale, {lt(c.get('at'))}]")
        accts = sorted(fs.get("accounts") or [], key=lambda a: a.get("fairshare") or 0)
        u = fs.get("users") or {}
        if len(accts) == 1:
            a = accts[0]
            line = f"fairshare {a.get('fairshare'):.2g} ({share_text(a)})"
        else:
            vals = [a["fairshare"] for a in accts if a.get("fairshare") is not None]
            line = f"fairshare {min(vals):.2g}-{max(vals):.2g} over {len(accts)} accounts" if vals else "fairshare ?"
        if fs.get("sees_others") and u.get("with_pending"):
            line += f"; lower than {u['higher_fairshare']} of {u['with_pending']} other users with queued jobs"
        out.append(f"{name}: {line}")
        if len(accts) > 1:
            out.append("  accounts: " + ", ".join(f"{a['account']} {a.get('fairshare'):.2g} ({share_text(a)})" for a in accts))
        w = sorted(((k, v) for k, v in (fs.get("weights") or {}).items() if v > 1), key=lambda kv: -kv[1])
        if w:
            lead = (f"mostly {w[0][0]}" if len(w) == 1 or w[0][1] >= 10 * w[1][1] else f"{w[0][0]} + {w[1][0]}")
            out.append(f"  priority is {lead} (weights " + ", ".join(f"{k} {kfmt(v)}" for k, v in w) + ")")
        mine = [j for j in state["jobs"] if j["cluster"] == c["name"] and j["state"] in PENDING and j.get("prio")]
        if not mine:
            out.append("  no pending jobs of yours")
            continue
        parts = {}
        for j in mine:
            for p, x in j["prio"]["parts"].items():
                parts.setdefault(p, []).append((j, x))
        lines = {}  # 内容一样的分区并成一行（比如 bluebear 一个 job 同时排三个分区）
        for p, lst in sorted(parts.items(), key=lambda kv: -len(kv[1])):
            pr = sorted(x["priority"] for _, x in lst)
            yours = kfmt(pr[0]) + (f"-{kfmt(pr[-1])}" if kfmt(pr[-1]) != kfmt(pr[0]) else "")
            st = (fs.get("partitions") or {}).get(p)
            if not fs.get("sees_others"):
                lines.setdefault(f"{len(lst)} queued, prio {yours} (other users' jobs not visible)", []).append(p)
            elif not st:
                lines.setdefault(f"{len(lst)} queued, prio {yours}; nobody else queued here", []).append(p)
            else:
                b = min((x for _, x in lst), key=lambda x: x["ahead"])
                lines.setdefault(f"{len(lst)} queued, prio {yours} vs others' median {kfmt(st['median'])}; "
                                 f"best is #{b['ahead'] + 1} of {st['competing'] + 1} ({b['users_ahead']} of {st['users']} users ahead)",
                                 []).append(p)
        out += [f"  {'/'.join(ps)}: {t}" for t, ps in lines.items()]
        if detail:
            f = max(mine, key=lambda j: j["prio"]["priority"])["prio"]
            out.append(f"  top job {f['priority']} = " + " + ".join(f"{k} {v}" for k, v in f["factors"].items() if v)
                       + (f" (qos {f['qos_name']})" if f.get("qos_name") else ""))
    if detail and out:
        out.append("\nfairshare: 0-1, higher = more priority; drops when you use more than your share, recovers as usage decays.\n"
                   "#N of M: position among eligible pending jobs in that partition (jobs on deps / held not counted; "
                   "backfill can still start small jobs out of order).")
    return "\n".join(out) or "no such cluster"


def job_text(state, cluster, jid):
    by_key = {(j["cluster"], j["id"]): j for j in state["jobs"]}
    j = by_key.get((cluster, jid))
    if not j:
        return f"job {cluster}:{jid} not in the current list (queue + recent history; GET /jobs?state=all&q={jid})"
    keys = ["id", "cluster", "name", "state", "reason", "qos", "partition", "nodelist", "nnodes", "cpus", "tres", "req_tres",
            "time_used", "time_limit", "submit", "eligible", "start", "end", "exit_code", "priority", "workdir",
            "dependency_raw", "source", "live"]
    if j["state"] in ACTIVE | PENDING:
        keys.remove("exit_code")  # sacct 给在跑的 job 也填 0:0，没意义
    lines = [f"{k:15} {j[k]}" for k in keys if j.get(k) not in (None, "", [])]
    if j.get("stale"):
        lines.append("(cluster unreachable: this is data from the last successful refresh)")
    lines.append(f"{'display state':15} {disp_state(j)}" + (f" (chain head is {'running' if j.get('dep_on') == 'run' else 'queued'})" if j.get("dep_on") else ""))
    if j["state"] in PENDING:
        lines.append(f"{'start estimate':15} {when_text(j)}")
        p = j.get("prio")
        if p:
            lines.append(f"{'priority':15} {p['priority']} = " + " + ".join(f"{k} {v}" for k, v in p["factors"].items() if v)
                         + f" (qos {p.get('qos_name')})")
            for part, x in sorted(p["parts"].items()):
                lines.append(f"{'  in ' + part:15} #{x['ahead'] + 1} of {x['competing'] + 1} eligible pending"
                             f" ({x['users_ahead']} other users ahead)" if x["competing"] else f"{'  in ' + part:15} nobody else queued")
        m = j.get("ml_est")
        if m:
            lines.append(f"{'model wait':15} q20 {fmt_h(m['wait'][0])}, q50 {fmt_h(m['wait'][1])}, q80 {fmt_h(m['wait'][2])} "
                         f"(partition {m.get('partition')}, {m.get('model')}, {m.get('n_seen')} batches seen)"
                         + (f"; deps ready by {ct(m.get('dep_ready'))}" if m.get("dep") else ""))
    for d in j.get("deps") or []:
        t = by_key.get((cluster, d.get("id")))
        lines.append(f"{'dependency':15} {d.get('type')}:{d.get('id')} ({d.get('status') or '-'}) -> "
                     + (f"{t['state']} {t.get('name')}" if t else "not in list"))
    ins = (state.get("inspect") or {}).get(f"{cluster}:{jid}")
    lines.append("")
    if not ins:
        pend = f"{cluster}:{jid}" in (state.get("inspect_pending") or [])
        lines.append("working folder: " + ("being read..." if pend else "not read yet (add ?read=1)"))
    elif ins.get("error"):
        lines.append(f"working folder: error: {en(ins['error'])}")
    else:
        size = "size ?" if ins.get("size_kb") is None else fmt_size(ins["size_kb"] * 1024)
        lines.append(f"working folder: {ins.get('dir')}  ({'exists' if ins.get('exists') else 'MISSING'}; {size}; "
                     f"{ins.get('count', '?')} entries; read {lt(ins.get('at'))})")
        if ins.get("shared"):
            lines.append(f"  same folder as: {' '.join(ins['shared'][:20])}")
        loop = ins.get("loop") or {}
        if loop.get("elec"):
            lines.append(f"  OUTCAR LOOP: cpu {loop['elec']['cpu']}s real {loop['elec']['real']}s"
                         + (f"; LOOP+ cpu {loop['ionic']['cpu']}s real {loop['ionic']['real']}s" if loop.get("ionic") else "")
                         + (f"  WARNING {en(loop['warn'])}" if loop.get("warn") else ""))
        for e in (ins.get("ls") or [])[:30]:
            lines.append(f"  {e.get('mtime', '')}  {e.get('size', 0):>12}  {e.get('name')}{'/' if e.get('is_dir') else ''}")
        for f in ins.get("files") or []:
            p = f.get("parsed") or {}
            summ = ", ".join(f"{k}={v}" for k, v in p.items() if k != "last_line" and v not in (None, ""))
            lines.append(f"\n--- {f.get('name')} (modified {f.get('mtime', '')}){'  ' + summ if summ else ''}")
            lines += [f"  {x}" for x in f.get("tail") or []]
    return "\n".join(lines)


def suggest_text(backend, args):
    req = {k: args.get(k) for k in ("cpus", "gpus", "gpu_type", "nodes", "time_h", "cluster") if args.get(k)}
    req.setdefault("cpus", 64)
    req.setdefault("time_h", 24)
    try:
        items = backend.suggest(req)
    except Exception as e:  # noqa: BLE001
        return f"prediction failed: {e}"
    if not items:
        st = backend.model_status()
        return "no model yet (train it on the web page, or wait for the automatic training)" if not st.get("meta") else "no partition fits this request"
    rows = [[i["cluster"], i["qos"], i["partition"], f"~{fmt_h(i['wait'][1])}", f"[{fmt_h(i['wait'][0])}-{fmt_h(i['wait'][2])}]",
             f"backlog <= {fmt_h(i['backlog'][1])}" if i.get("backlog") else "",
             f"mine {i.get('mine_running', 0)}/{i.get('mine_pending', 0)}",
             "little data" if i.get("n_seen", 0) < 3 else f"{i.get('n_seen')} batches", "stale" if i.get("stale") else ""]
            for i in items]
    return (f"new job: {req.get('cpus')} cpus, {req.get('gpus', 0)} gpus, {req.get('nodes', 1)} node(s), {req.get('time_h')} h"
            " -- fastest first (model median wait [q20-q80])\n"
            + table(rows, ["cluster", "qos", "partition", "wait", "range", "upper bound", "mine r/p", "data", ""]))


# ---------------- 路由 ----------------
def blueprint(backend):
    bp = Blueprint("text", __name__)

    def txt(s, code=200):
        return Response(s.rstrip() + "\n", status=code, mimetype="text/plain; charset=utf-8")

    @bp.route("/")
    def overview():
        return txt(overview_text(backend.get_state()))

    @bp.route("/help")
    def help_():
        return txt(HELP)

    @bp.route("/jobs")
    def jobs():
        state = backend.get_state()
        js = filter_jobs(state, request.args)
        limit = request.args.get("limit", type=int)
        if request.args.get("flat"):
            js.sort(key=lambda j: (j["cluster"], sort_jobid(j["id"])))
            more = f"\n... {len(js) - limit} more" if limit and len(js) > limit else ""
            body = table([job_line(j) for j in js[:limit]], JOB_HEADER) + more
        else:
            body = groups_text(js, limit)
        return txt(f"{header_text(state)}\n{len(js)} jobs (state={request.args.get('state', 'active')})\n\n{body}")

    @bp.route("/job/<cluster>/<jid>")
    def job(cluster, jid):
        key = f"{cluster}:{jid}"
        if request.args.get("read"):
            backend.enqueue_inspect([key], force=True)
            deadline = time.time() + 90
            while time.time() < deadline:
                time.sleep(1)
                if key not in (backend.get_state().get("inspect_pending") or []):
                    break
        return txt(job_text(backend.get_state(), cluster, jid))

    @bp.route("/done")
    def done():
        return txt(done_text(backend.get_state()))

    @bp.route("/alerts")
    def alerts():
        return txt(alerts_text(backend.get_state()))

    @bp.route("/qos")
    def qos():
        return txt(qos_text(backend.get_state(), request.args.get("cluster")))

    @bp.route("/fairshare")
    def fairshare():
        return txt(fairshare_text(backend.get_state(), request.args.get("cluster"), detail=True))

    @bp.route("/suggest")
    def suggest():
        return txt(suggest_text(backend, request.args))

    @bp.route("/refresh", methods=["POST"])
    def refresh():
        backend.enqueue_refresh(force=True, manual=True)
        if request.args.get("wait", "1") != "0":
            deadline = time.time() + 600
            time.sleep(1)
            while time.time() < deadline and backend.get_status()["progress"]["refresh_queued"]:
                time.sleep(2)
        return txt(overview_text(backend.get_state()))

    return bp
