"""
排队积压估计（从 sulis 上用的 godot.py 移植过来）
================================================
思路：把每个分区里所有 job 的 (剩余时长 × 核数/卡数) 加起来，除以分区总核数/卡数，
得到"假设完美排满，要多少小时才能把现在的活清空"。
- Local  = 只算这个分区自己的 job / 自己的容量
- Global = 算上和它共享同一批节点的所有分区（比如 gpu / gpulowpri / gpu-devel 共用一批卡）
真实等待时间一般落在这个范围里；你自己的 job 取最慢的那个资源（瓶颈）。

需要 squeue --all 能看到所有人的 job，sinfo 能看到分区；有些集群（PrivateData）
看不到，就没法估计。
"""

import re

from i18n import L

DEFAULT_TIMELIMIT_HOURS = 24.0

# 拼进 app.py 那次 SSH 里的命令
COMMANDS = [
    ("BL_NODES", "sinfo -h -N -o '%N|%P|%G'"),
    ("BL_CAPS", "sinfo -a -h -o '%P|%C'"),
    ("BL_JOBS", "squeue --all -h -o '%i|%P|%T|%C|%b|%l|%L|%u|%a'"),
]

RUNNING_STATES = {"RUNNING", "COMPLETING", "CONFIGURING"}


def parse_duration(s):
    """Slurm 时长 -> 小时。UNLIMITED / N/A 当 24h 算。"""
    if not s or s.upper() in ("UNLIMITED", "N/A", "INVALID", "NOT_SET"):
        return DEFAULT_TIMELIMIT_HOURS
    try:
        days = 0
        if "-" in s:
            d, s = s.split("-", 1)
            days = int(d)
        parts = [int(p) for p in s.split(":")]
        if len(parts) == 3:
            h, m, sec = parts
        elif len(parts) == 2:
            h, m, sec = (0, parts[0], parts[1]) if days == 0 else (parts[0], parts[1], 0)
        else:
            h, m, sec = (parts[0], 0, 0) if days else (0, parts[0], 0)
        return days * 24.0 + h + m / 60.0 + sec / 3600.0
    except ValueError:
        return 1.0


def parse_gpu(tres):
    """'gres/gpu:a100:1' / 'gpu:a100:8(S:1,3,5,7)' / 'gres/gpu:1' -> (type 或 None, 数量)"""
    if not tres or tres in ("(null)", "N/A"):
        return None, 0
    tres = re.sub(r"\([^)]*\)", "", tres.lower())  # 去掉 (S:1,3,5,7) 这种 socket 信息
    for seg in tres.split(","):
        m = re.search(r"gpu[:=]([a-z][\w.-]*)[:=](\d+)", seg)
        if m:
            return m.group(1), int(m.group(2))
        m = re.search(r"gpu[:=](\d+)", seg)
        if m:
            return None, int(m.group(1))
    return None, 0


def base_id(jobid):
    m = re.match(r"(\d+)", jobid)
    return int(m.group(1)) if m else 0


def analyze(sections, me):
    """sections: {"BL_NODES": [...], "BL_CAPS": [...], "BL_JOBS": [...]}，me: 本人用户名（从 squeue 里认）
    返回给前端的 dict，外加 per-partition 统计供 estimate_job() 用。"""
    # ---- 拓扑：分区共享关系 & GPU 池 ----
    node_parts, node_gpu = {}, {}
    for line in sections.get("BL_NODES", []):
        parts = line.split("|")
        if len(parts) != 3:
            continue
        node, part, gres = parts
        node_parts.setdefault(node, set()).add(part.rstrip("*"))
        gtype, gcount = parse_gpu(gres)
        if gcount:
            node_gpu[node] = (gtype or "gpu", gcount)
    overlap, pools = {}, {}
    for node, ps in node_parts.items():
        for p in ps:
            overlap.setdefault(p, set()).update(ps)
        if node in node_gpu:
            gtype, gcount = node_gpu[node]
            pool = pools.setdefault(gtype, {"cards": 0, "partitions": set()})
            pool["cards"] += gcount
            pool["partitions"].update(ps)

    caps = {}
    for line in sections.get("BL_CAPS", []):
        parts = line.split("|")
        if len(parts) == 2:
            try:
                caps[parts[0].rstrip("*")] = int(parts[1].split("/")[-1])
            except ValueError:
                pass

    if not caps:
        return {"ok": False, "error": L("sinfo 看不到分区信息，没法估计",
                                        "sinfo shows no partitions, can't estimate")}, None

    # ---- 作业 ----
    jobs = []
    for line in sections.get("BL_JOBS", []):
        p = line.split("|")
        if len(p) != 9:
            continue
        gtype, gcount = parse_gpu(p[4])
        limit_h = parse_duration(p[5])
        running = p[2] in RUNNING_STATES
        try:
            cpus = int(p[3])
        except ValueError:
            cpus = 0
        jobs.append({
            "id": p[0], "id_num": base_id(p[0]), "parts": p[1].split(","), "state": p[2],
            "running": running, "cpus": cpus, "gpu_type": gtype, "gpus": gcount,
            "limit": p[5], "limit_h": limit_h,
            "left_h": parse_duration(p[6]) if running else limit_h,
            "user": p[7], "account": p[8],
        })
    users = {j["user"] for j in jobs}
    if jobs and users <= {me}:
        return {"ok": False, "private": True,
                "error": L("这个集群的 squeue 只能看到你自己的 job，没法估计别人占了多少",
                           "squeue only shows your own jobs here, can't estimate others' load")}, None

    def gpu_types_for(part, gtype):
        if gtype and gtype in pools:
            return [gtype]
        return [t for t, info in pools.items() if part in info["partitions"]]

    cpu_local = {p: 0.0 for p in caps}
    gpu_local = {p: {} for p in caps}
    gpu_global = {t: 0.0 for t in pools}
    my_first_pending = {}
    mine = {}
    for j in jobs:
        # 排队时可以列多个分区（bluebear 那种），工作量平均摊到每个分区上
        share = 1.0 / len(j["parts"])
        for p in j["parts"]:
            cpu_local[p] = cpu_local.get(p, 0.0) + j["left_h"] * j["cpus"] * share
            if j["gpus"]:
                for t in gpu_types_for(p, j["gpu_type"]):
                    w = j["left_h"] * j["gpus"] * share
                    gpu_local.setdefault(p, {})
                    gpu_local[p][t] = gpu_local[p].get(t, 0.0) + w
                    gpu_global[t] = gpu_global.get(t, 0.0) + w
            if j["user"] == me:
                m = mine.setdefault(p, {"running": 0, "pending": 0})
                m["running" if j["running"] else "pending"] += 1
                if not j["running"]:
                    my_first_pending[p] = min(my_first_pending.get(p, j["id_num"]), j["id_num"])

    stats = {}
    for p in caps:
        shared = overlap.get(p, {p}) | {p}
        g_cap = sum(caps.get(sp, 0) for sp in shared) or caps[p]
        c_min = cpu_local.get(p, 0.0) / max(caps[p], 1)
        c_max = max(c_min, sum(cpu_local.get(sp, 0.0) for sp in shared) / max(g_cap, 1))
        gpu = []
        for t in gpu_types_for(p, None):
            cards = pools[t]["cards"]
            g_min = gpu_local.get(p, {}).get(t, 0.0) / max(cards, 1)
            gpu.append({"type": t, "min": g_min, "max": max(g_min, gpu_global.get(t, 0.0) / max(cards, 1))})
        stats[p] = {"cpu_min": c_min, "cpu_max": c_max, "gpu": gpu}

    partitions = [{
        "name": p,
        "cpus": caps[p],
        "gpus": sum(pools[g["type"]]["cards"] for g in stats[p]["gpu"]),
        "cpu_min": stats[p]["cpu_min"], "cpu_max": stats[p]["cpu_max"],
        "gpu": stats[p]["gpu"],
        "mine_running": mine.get(p, {}).get("running", 0),
        "mine_pending": mine.get(p, {}).get("pending", 0),
    } for p in sorted(caps)]

    # ---- 插队：比你在同分区最早排队的 job 晚提交、却已经在跑的 ----
    cutters = []
    for j in jobs:
        if not j["running"] or j["user"] == me:
            continue
        p = j["parts"][0]
        if p in my_first_pending and j["id_num"] > my_first_pending[p]:
            reasons = []  # (中文, English)
            if j["limit_h"] < 4.0:
                reasons.append(("时间短", "short time limit"))
            if j["gpus"] == 0:
                reasons.append(("只要 CPU", "CPU only"))
            elif j["gpus"] == 1:
                reasons.append(("只要 1 卡", "single GPU"))
            cutters.append({
                "id": j["id"], "partition": p, "account": j["account"], "cpus": j["cpus"],
                "gpus": j["gpus"], "limit": j["limit"], "reason": L("、".join(r[0] for r in reasons) or "Backfill",
                                                      ", ".join(r[1] for r in reasons) or "Backfill"),
            })
    cutters.sort(key=lambda c: -base_id(c["id"]))

    result = {
        "ok": True,
        "partitions": partitions,
        "pools": [{"type": t, "cards": info["cards"], "partitions": sorted(info["partitions"])}
                  for t, info in sorted(pools.items())],
        "cutters_total": len(cutters),
        "cutters": cutters[:30],
    }
    return result, {"stats": stats, "pools": {t: sorted(i["partitions"]) for t, i in pools.items()}}


def estimate_job(job, model):
    """给自己的一个排队 job 估计等待范围。job 需要 partition / cpus / tres。
    多个候选分区时取最快的那个（Slurm 会在哪个先有空就去哪个）。"""
    if not model:
        return None
    gtype, gcount = parse_gpu(job.get("tres", ""))
    best = None
    for p in job.get("partition", "").split(","):
        st = model["stats"].get(p)
        if not st:
            continue
        ests = [{"label": "CPU", "min": st["cpu_min"], "max": st["cpu_max"]}]
        if gcount:
            for g in st["gpu"]:
                if gtype is None or g["type"] == gtype:
                    ests.append({"label": f"GPU {g['type']}", "min": g["min"], "max": g["max"]})
        ests.sort(key=lambda e: e["max"])
        cand = {"partition": p, "bottleneck": ests[-1], "all": ests}
        if best is None or cand["bottleneck"]["max"] < best["bottleneck"]["max"]:
            best = cand
    return best
