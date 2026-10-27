"""
排队等待预测（小型机器学习模型）
==============================
用 waitlog/ 里攒下来的数据，学"在这种集群状态下，这个 job 从可以排队起还要等多久"。

模型：scikit-learn 的 HistGradientBoostingRegressor（梯度提升树），分位数损失，
训练三个：q20 / q50 / q80 → "大概 X 小时，多半在 A–B 之间"。每个集群单独一组模型
（不同集群能拿到的特征都不一样）；已经开始的 job 少于 MIN_CLUSTER_STARTED 的集群用全局模型。
QoS / 分区是模型里的分类特征（单独给每个队列训练数据太少，会严重过拟合）。
选它的原因：
- 数据少、特征混杂（分类 + 数值）、很多缺失（bluebear 看不到全集群、SGE 没有积压估计）——
  树模型原生支持缺失值和分类特征，不用填补；排队数到某个量级等待才暴涨这种非线性也抓得住
- 便宜：几千行训练（含回测）几秒，单线程；预测几百个 job 几十毫秒
等待时间跨度很大（几秒到几百小时），在 log 空间里训练。树很浅、叶子很大（GB_PARAMS），
因为真正独立的样本不多：成批提交的 job 等待时间几乎一样，所以一批合起来只算一份权重；
集群忙闲会变，越近的数据权重越大（半衰期 wait_model_half_life_days 天）。

样本：一律从 eligible 算起（带 dependency 的 job，前置 job 结束之前排到队头也不会开始，
那段时间不算排队；实测 eligible 和前置 job 结束时间基本只差几分钟）。
- eligible 之后第一次看到它排队：特征用当时记下的状态（obs_*），目标 = 从那一刻起还要等多久，
  已等时间也是特征
- 否则（开得太快没看到 / 看到时还在等 dependency）：用 eligible 前后 2 小时内最近的
  qos / 分区快照当"当时的状态"，已等 0
- 没开始（还在排 / 排队时被取消，censored=1）：只知道"至少要等这么久"。先拿下界当目标训练，
  再把目标换成 max(下界, 预测) 重训一次（删失分位数回归的常见近似），不然模型会偏乐观

预测：排队 job 的开始时间 = 可以排队的时间 + 预测的排队时间。还在等 dependency 的，
可以排队的时间 = 前置 job 预计结束（在跑的：开始 + 时限；在排的：递归它的预计开始 + 时限），
所以是偏保守的上限。

评估：按时间滚动回测（最早 50% 起步，每次往后预测 12.5%），和 baseline 比：
同集群同 QoS 历史中位数，以及（有的话）Slurm 自己的估计、积压估计。结果存在模型里，网页上能看到。

scikit-learn 是可选依赖：没装的话页面上的预测功能显示"需要安装 scikit-learn"。
命令行：python predictor.py [waitlog 目录] → 训练并打印回测结果（不保存）。
"""

import bisect
import csv
import math
import os
import pickle
import threading
import time
from datetime import datetime

import backlog
from i18n import L
from schedulers.base import ACTIVE, PENDING, waiting_on_dependency

QUANTILES = (0.2, 0.5, 0.8)
FLOOR_H = 1 / 60  # log 变换的下限：1 分钟
MIN_TRAIN_ROWS = 50  # 已经开始的 job 少于这么多就不训练
MIN_CLUSTER_STARTED = 30  # 少于这么多的集群用全局模型
FEW_SEEN = 3  # 历史上在这个 QoS + 分区开始过的 job 少于这么多批：标"数据少"（建议里排后面）
TRAIN_THREADS = 2
HISTORY_KEEP = 30  # 训练记录保留几条（看模型是不是越来越准）
GB_PARAMS = dict(max_iter=40, learning_rate=0.1, max_depth=3, l2_regularization=5.0)
LEAF_FRACTION = 25  # 每片叶子至少 样本数/25 个样本（至少 8 个）：数据少的集群也能分叉，又不会背下单个批次

NUM_FEATURES = [
    "cpus", "gpus", "nodes", "time_limit_h", "mem_gb", "n_partitions",
    "waited_h", "has_dep", "dep_wait_h",
    "qos_all_running", "qos_all_pending", "qos_mine_running", "qos_mine_pending",
    "part_all_running", "part_all_pending",
    "bl_cpu_min_h", "bl_cpu_max_h", "bl_gpu_min_h", "bl_gpu_max_h",
    "slurm_remaining_h",
]
CAT_FEATURES = ["cluster", "qos", "partition", "reason"]
FEATURES = NUM_FEATURES + CAT_FEATURES


# ---------------- 小工具 ----------------
def num(v):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return math.nan
    return x if math.isfinite(x) else math.nan


def parse_ts(s):
    try:
        return datetime.strptime((s or "")[:19], "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return None


def mem_gb(s):
    """'448G' / '40000M' / '1T' -> GB"""
    s = (s or "").strip().upper()
    if not s:
        return math.nan
    unit = s[-1]
    x = num(s[:-1] if unit.isalpha() else s)
    return x * {"K": 1 / 1048576, "M": 1 / 1024, "G": 1, "T": 1024}.get(unit, 1 / 1024)


def norm_reason(r):
    """squeue 的 reason 归几类；具体的节点名、时间之类不要"""
    r = (r or "").strip("()")
    if not r or r == "None":
        return ""
    if r.startswith(("QOS", "Assoc")):
        return "Limit"
    if r in ("Priority", "Resources", "Dependency", "BeginTime", "JobHeldUser", "JobHeldAdmin",
             "ReqNodeNotAvail", "PartitionTimeLimit"):
        return r
    if r.startswith("ReqNodeNotAvail"):
        return "ReqNodeNotAvail"
    if r.startswith("Dependency"):  # SGE: 'Dependency (hqw)'
        return "Dependency"
    return "Other"


def main_partition(row):
    """排队时可能是 'a,b,c'：开始后用实际跑的分区，否则用积压估计选的，否则第一个"""
    parts = [p for p in (row.get("partition") or "").split(",") if p]
    for p in (row.get("run_partition"), row.get("obs_bl_partition")):
        if p and (not parts or p in parts):
            return p
    return parts[0] if parts else ""


def fmt_q(h):
    return None if h is None or not math.isfinite(h) else round(max(h, 0.0), 3)


# ---------------- 训练样本 ----------------
SNAP_GAP_H = 2  # eligible 前后多久以内的快照算"当时的状态"


class Snapshots:
    """qos_snapshots.csv / partition_snapshots.csv：按时间找离某一刻最近的集群状态。
    给"没看到它排队"或者"第一次看到它时还在等 dependency"的 job 补上 eligible 那一刻的状态"""

    def __init__(self, directory=None):
        self.qos, self.part = {}, {}  # key -> ([时间戳], [行])
        if not directory:
            return
        for name, key, store in (("qos_snapshots.csv", ("cluster", "qos"), self.qos),
                                 ("partition_snapshots.csv", ("cluster", "partition", "gpu_type"), self.part)):
            try:
                rows = load_rows(os.path.join(directory, name))
            except OSError:
                continue
            for r in rows:
                try:  # observed_at 是本机时间；早期的分区快照只有 backlog_at（UTC）
                    t = (parse_ts(r["observed_at"]) if r.get("observed_at")
                         else datetime.fromisoformat(r["backlog_at"])).timestamp()
                except (TypeError, ValueError, AttributeError, KeyError):
                    continue
                ts, rs = store.setdefault(tuple(r.get(k, "") for k in key), ([], []))
                ts.append(t); rs.append(r)
        for store in (self.qos, self.part):
            for k, (ts, rs) in store.items():
                order = sorted(range(len(ts)), key=ts.__getitem__)
                store[k] = ([ts[i] for i in order], [rs[i] for i in order])

    @staticmethod
    def _near(store, key, t):
        ts, rs = store.get(key, ([], []))
        i = bisect.bisect_left(ts, t)
        best = min((k for k in (i - 1, i) if 0 <= k < len(ts)), key=lambda k: abs(ts[k] - t), default=None)
        return rs[best] if best is not None and abs(ts[best] - t) <= SNAP_GAP_H * 3600 else None

    def features(self, cluster, qos, partition, gpus, gpu_type, when):
        t = when.timestamp()
        f = {}
        q = self._near(self.qos, (cluster, qos), t)
        if q:
            f.update({"qos_all_running": num(q["all_running"]), "qos_all_pending": num(q["all_pending"]),
                      "qos_mine_running": num(q["mine_running"]), "qos_mine_pending": num(q["mine_pending"])})
        p = self._near(self.part, (cluster, partition, ""), t)
        if p:
            f["part_all_running"], f["part_all_pending"] = num(p.get("all_running")), num(p.get("all_pending"))
            f["bl_cpu_min_h"], f["bl_cpu_max_h"] = num(p["cpu_min_h"]), num(p["cpu_max_h"])
            if gpus:
                types = [gt for (c, pp, gt) in self.part if c == cluster and pp == partition and gt
                         and (not gpu_type or gt == gpu_type)]
                gs = [g for g in (self._near(self.part, (cluster, partition, gt), t) for gt in types) if g]
                if gs:
                    g = min(gs, key=lambda g: num(g["gpu_max_h"]))
                    f["bl_gpu_min_h"], f["bl_gpu_max_h"] = num(g["gpu_min_h"]), num(g["gpu_max_h"])
        return f


def row_to_sample(row, snaps):
    """jobs.csv 的一行 -> (features dict, target 小时, censored, 时间) 或 None

    一律从 eligible（dependency 满足、真正开始排队）算起：带 dependency 的 job，
    前置 job 没结束之前排到队头也不会开始，那段时间不是"排队"。
    - 第一次看到它排队是在 eligible 之后（不是在等 dependency）：用当时记下的状态（obs_*），
      目标 = 从那一刻起还要等多久，已等时间也是特征
    - 否则（开得太快没看到 / 看到时还在等 dependency）：用 eligible 那一刻前后的快照，已等 0
    - 带 dependency 但还没有 eligible（还在等前置 job，或者等的时候被取消了）：不算"""
    if row.get("dependency") and not row.get("eligible"):
        return None
    eligible = parse_ts(row.get("eligible") or row.get("submit"))
    submit = parse_ts(row.get("submit"))
    total = num(row.get("wait_from_eligible_h"))
    if eligible is None or math.isnan(total) or total < 0:
        return None
    censored = row.get("censored") == "1"
    obs_at = parse_ts(row.get("obs_at")) if row.get("seen_pending") == "1" else None
    reason = norm_reason(row.get("obs_reason"))
    use_obs = obs_at is not None and obs_at >= eligible and reason != "Dependency"
    waited = (obs_at - eligible).total_seconds() / 3600 if use_obs else 0.0
    target = total - waited
    if target < 0:
        if censored:
            return None
        target = 0.0
    when = obs_at if use_obs else eligible
    gpus = num(row.get("gpus") or 0)
    f = {
        "cluster": row.get("cluster", ""), "qos": row.get("qos", ""),
        "partition": main_partition(row),
        "reason": reason if use_obs else "",
        "cpus": num(row.get("cpus")), "gpus": gpus,
        "nodes": num(row.get("nodes")), "time_limit_h": num(row.get("time_limit_h")),
        "mem_gb": mem_gb(row.get("mem")),
        "n_partitions": len([p for p in (row.get("partition") or "").split(",") if p]) or 1,
        "waited_h": waited,
        # 等过 dependency 的 job：交得早，优先级（age）已经攒起来了，依赖一满足往往很快开始
        "has_dep": 1.0 if row.get("dependency") else 0.0,
        "dep_wait_h": max((eligible - submit).total_seconds() / 3600, 0)
                      if row.get("dependency") and submit else 0.0,
    }
    if use_obs:
        est = parse_ts(row.get("obs_slurm_est_start"))
        f.update({
            "qos_all_running": num(row.get("obs_qos_all_running")),
            "qos_all_pending": num(row.get("obs_qos_all_pending")),
            "qos_mine_running": num(row.get("obs_qos_mine_running")),
            "qos_mine_pending": num(row.get("obs_qos_mine_pending")),
            "part_all_running": num(row.get("obs_part_all_running")),
            "part_all_pending": num(row.get("obs_part_all_pending")),
            "bl_cpu_min_h": num(row.get("obs_bl_cpu_min_h")),
            "bl_cpu_max_h": num(row.get("obs_bl_cpu_max_h")),
            "bl_gpu_min_h": num(row.get("obs_bl_gpu_min_h")),
            "bl_gpu_max_h": num(row.get("obs_bl_gpu_max_h")),
            "slurm_remaining_h": (est - obs_at).total_seconds() / 3600 if est else math.nan,
        })
    elif snaps:
        f.update(snaps.features(f["cluster"], f["qos"], f["partition"], gpus,
                                row.get("gpu_type", ""), when))
    return f, target, censored, when


def load_rows(path):
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


# ---------------- 编码 ----------------
class Encoder:
    """分类特征 -> 整数（HistGradientBoosting 要求 0..n-1），没见过的 -> NaN"""

    def __init__(self, samples):
        self.vocab = {}
        for c in CAT_FEATURES:
            vals = sorted({s[c] for s in samples if s.get(c)})
            self.vocab[c] = {v: i for i, v in enumerate(vals)}

    def matrix(self, samples):
        import numpy as np
        X = np.full((len(samples), len(FEATURES)), np.nan)
        for i, s in enumerate(samples):
            for k, name in enumerate(FEATURES):
                v = s.get(name)
                if name in self.vocab:
                    X[i, k] = self.vocab[name].get(v, np.nan) if v else np.nan
                elif v is not None:
                    X[i, k] = v
        return X

    def cat_mask(self):
        return [name in self.vocab for name in FEATURES]


def to_y(h):
    import numpy as np
    return np.log(np.maximum(h, 0) + FLOOR_H)


def from_y(y):
    import numpy as np
    return np.maximum(np.exp(y) - FLOOR_H, 0)


def sample_weights(samples, times, half_life_days):
    """同一批提交的 job（同集群/QoS/分区/核数，半小时内）合起来算一份；越旧的权重越小"""
    import numpy as np
    keys = [(s["cluster"], s["qos"], s["partition"], s["cpus"], int(t // 1800))
            for s, t in zip(samples, times)]
    size = {}
    for k in keys:
        size[k] = size.get(k, 0) + 1
    w = np.array([1.0 / size[k] for k in keys])
    if half_life_days and len(times):
        w *= 0.5 ** ((np.max(times) - np.asarray(times)) / 86400 / half_life_days)
    # 归一到平均 1：分位数损失的 hessian = 权重之和，太小的话 l2_regularization 会把叶子值压没
    return w * len(w) / w.sum() if len(w) else w


class QuantileModel:
    """一组样本上的 q20/q50/q80 三个模型（一个集群一组，再加一个全局的兜底）"""

    def __init__(self, samples, target, censored, weight, iterations=2):
        import numpy as np
        from sklearn.ensemble import HistGradientBoostingRegressor
        self.enc = Encoder(samples)
        X = self.enc.matrix(samples)
        y0 = to_y(target)
        self.models = {}
        for q in QUANTILES:
            # 没开始的 job：先拿"至少等了这么久"当目标，再换成 max(下界, 预测) 重训
            y = y0.copy()
            for it in range(iterations if censored.any() else 1):
                m = HistGradientBoostingRegressor(loss="quantile", quantile=q, **GB_PARAMS,
                                                  min_samples_leaf=max(8, len(samples) // LEAF_FRACTION),
                                                  categorical_features=self.enc.cat_mask(),
                                                  random_state=0)
                m.fit(X, y, sample_weight=weight)
                y = np.where(censored, np.maximum(y0, m.predict(X)), y0)
            self.models[q] = m

    def predict(self, samples):
        """-> 每行 (q20, q50, q80) 小时，保证单调"""
        import numpy as np
        X = self.enc.matrix(samples)
        P = np.column_stack([from_y(self.models[q].predict(X)) for q in QUANTILES])
        return np.sort(P, axis=1)


class HistoryModel:
    """不看集群状态，只看历史：同 (集群, QoS, 分区) 已经开始的 job 等了多久的 q20/q50/q80
    （批次 + 近期加权）；数据不够就退到 (集群, QoS)、集群。回测里当 baseline，
    哪个集群它比模型准就用它"""

    KEYS = (("cluster", "qos", "partition"), ("cluster", "qos"), ("cluster",), ())

    def __init__(self, samples, target, censored, weight, min_weight=3):
        groups = {}
        for s, t, c, w in zip(samples, target, censored, weight):
            if not c:
                for ks in self.KEYS:
                    groups.setdefault(tuple(s[k] for k in ks), []).append((t, w))
        self.q = {}
        for key, vals in groups.items():
            vals.sort()
            tot = sum(w for _, w in vals)
            if tot < min_weight * 0.5 ** 4 and key:  # 权重太小（太少 / 太旧）的组不用
                continue
            out, acc, i = [], 0.0, 0
            for q in QUANTILES:
                while i < len(vals) - 1 and acc + vals[i][1] < q * tot:
                    acc += vals[i][1]
                    i += 1
                out.append(float(vals[i][0]))
            self.q[key] = out

    def predict(self, samples):
        import numpy as np
        return np.array([next((self.q[k] for k in (tuple(s[x] for x in ks) for ks in self.KEYS)
                               if k in self.q), [0.0, 0.0, 0.0]) for s in samples])


class WaitModel:
    """每个集群一个 QuantileModel（数据太少的集群用全局的）+ 一个 HistoryModel；
    choice[集群] = 回测里哪个更准（"model" / "history"），没回测过的用 model"""

    def __init__(self, samples, target, censored, times, half_life_days):
        import numpy as np
        w = sample_weights(samples, times, half_life_days)
        self.glob = QuantileModel(samples, target, censored, w)
        self.hist = HistoryModel(samples, target, censored, w)
        self.per = {}
        self.choice = {}
        cl = np.array([s["cluster"] for s in samples])
        for c in sorted(set(cl)):
            m = cl == c
            if (~censored[m]).sum() >= MIN_CLUSTER_STARTED:
                self.per[c] = QuantileModel([s for s, k in zip(samples, m) if k],
                                            target[m], censored[m], w[m])

    def which(self, cluster):
        """给前端看：cluster = 这个集群自己的模型，global = 全局模型，history = 历史分位数"""
        if self.choice.get(cluster) == "history":
            return "history"
        return "cluster" if cluster in self.per else "global"

    def predict(self, samples, force=None):
        """force="model" / "history"：回测时两个都要算"""
        import numpy as np
        P = np.zeros((len(samples), 3))
        groups = {}
        for i, s in enumerate(samples):
            c = s["cluster"]
            kind = force or self.choice.get(c, "model")
            groups.setdefault("history" if kind == "history" else c if c in self.per else None, []).append(i)
        for key, idx in groups.items():
            m = self.hist if key == "history" else self.per[key] if key else self.glob
            P[idx] = m.predict([samples[i] for i in idx])
        return P


# ---------------- 评估：按时间滚动回测 ----------------
def wmedian(x, w):
    import numpy as np
    o = np.argsort(x)
    c = np.cumsum(w[o])
    return float(x[o][np.searchsorted(c, c[-1] / 2)])


def score(pred, actual, w, lo=None, hi=None):
    """误差中位数（小时）、"差不多对"的比例（误差在 2 倍以内或 15 分钟以内）、
    q20–q80 区间罩住真实值的比例（理想是 60%）。都按批加权：一批一起交的 job 算一份"""
    import numpy as np
    if not len(actual):
        return None
    err = np.abs(pred - actual)
    ok = (err <= 0.25) | ((pred <= 2 * actual) & (actual <= 2 * pred))
    out = {"n": int(len(actual)), "mae_h": round(wmedian(err, w), 2),
           "hit": round(float((ok * w).sum() / w.sum()), 3)}
    if lo is not None:
        cov = (actual >= lo) & (actual <= hi)
        out["coverage"] = round(float((cov * w).sum() / w.sum()), 3)
    return out


def backtest(samples, target, censored, times, half_life_days, folds=4):
    """最早 50% 起步，每次多用 12.5% 训练、预测接下来 12.5% 里已经开始的 job。
    比较：模型 / 历史分位数（同 QoS + 分区）/ Slurm 自己的估计 / 积压估计（后两个有的话）"""
    import numpy as np
    order = np.argsort(times)
    n = len(order)
    bw = sample_weights(samples, times, 0)
    idx, PM, PH = [], [], []
    for k in range(folds):
        a, b = int(n * (0.5 + 0.5 * k / folds)), int(n * (0.5 + 0.5 * (k + 1) / folds))
        tr, te = order[:a], order[a:b]
        te = te[~censored[te]]
        if (~censored[tr]).sum() < MIN_TRAIN_ROWS or not len(te):
            continue
        model = WaitModel([samples[i] for i in tr], target[tr], censored[tr], times[tr], half_life_days)
        test = [samples[i] for i in te]
        idx.extend(te)
        PM.append(model.predict(test, "model"))
        PH.append(model.predict(test, "history"))
    if len(idx) < 10:
        return None
    idx, PM, PH = np.array(idx), np.vstack(PM), np.vstack(PH)
    actual, w = target[idx], bw[idx]
    cl = np.array([samples[i]["cluster"] for i in idx])

    def block(m):
        out = {"model": score(PM[m, 1], actual[m], w[m], PM[m, 0], PM[m, 2]),
               "history": score(PH[m, 1], actual[m], w[m], PH[m, 0], PH[m, 2])}
        for key, fn in (("slurm", lambda s: s.get("slurm_remaining_h", math.nan)),
                        ("backlog", lambda s: max(num(s.get("bl_cpu_max_h")), num(s.get("bl_gpu_max_h"))))):
            sub = np.array([k for k in np.flatnonzero(m) if not math.isnan(fn(samples[idx[k]]))], int)
            if len(sub) >= 5:
                out[key] = score(np.array([max(fn(samples[idx[k]]), 0) for k in sub]), actual[sub], w[sub])
                out[f"model_on_{key}"] = score(PM[sub, 1], actual[sub], w[sub])
        return out

    return {"all": block(np.ones(len(idx), bool)),
            "clusters": {c: block(cl == c) for c in sorted(set(cl))},
            "from": datetime.fromtimestamp(times[order[n // 2]]).strftime("%Y-%m-%d")}


# ---------------- 训练 + 存取 ----------------
def train(rows, snaps=None, half_life_days=14, now=None):
    """rows: jobs.csv 的行。返回 bundle（可以 pickle）"""
    import numpy as np
    import sklearn
    from threadpoolctl import threadpool_limits
    now = now or datetime.now()
    t0 = time.time()
    samples, target, censored, times = [], [], [], []
    for r in rows:
        s = row_to_sample(r, snaps)
        if s:
            samples.append(s[0]); target.append(s[1]); censored.append(s[2])
            times.append(s[3].timestamp())
    if sum(1 for c in censored if not c) < MIN_TRAIN_ROWS:
        raise ValueError(L(f"数据太少：只有 {len(samples)} 条可用记录（已经开始的至少要 {MIN_TRAIN_ROWS} 条）",
                           f"Not enough data: {len(samples)} usable rows (need {MIN_TRAIN_ROWS} started jobs)"))
    target, censored, times = np.array(target), np.array(censored), np.array(times)
    with threadpool_limits(TRAIN_THREADS):  # 数据很小，多线程反而浪费
        metrics = backtest(samples, target, censored, times, half_life_days)
        model = WaitModel(samples, target, censored, times, half_life_days)
    for c, m in ((metrics or {}).get("clusters") or {}).items():
        # 回测里历史分位数明显更准（"差不多对"的比例高 2% 以上）的集群就用它
        if m["history"]["hit"] > m["model"]["hit"] + 0.02:
            model.choice[c] = "history"
    # 每个 (集群, QoS, 分区) 有多少批已经开始的 job（一起交的算一批）：太少的标"数据少"
    batches, per_cluster = {}, {}
    for s, c, t in zip(samples, censored, times):
        k = f"{s['cluster']}|{s['qos']}|{s['partition']}"
        b = batches.setdefault(k, set())
        if not c:
            b.add((s["cpus"], int(t // 1800)))
    counts = {k: len(v) for k, v in batches.items()}
    for s, c in zip(samples, censored):
        pc = per_cluster.setdefault(s["cluster"], {"samples": 0, "started": 0, "with_state": 0})
        pc["samples"] += 1
        pc["started"] += 0 if c else 1
        pc["with_state"] += "qos_mine_pending" in s or "bl_cpu_max_h" in s
    for c, pc in per_cluster.items():
        pc["model"] = model.which(c)
    meta = {
        "trained_at": now.strftime("%Y-%m-%dT%H:%M:%S"),
        "n_samples": len(samples), "n_started": int((~censored).sum()),
        "n_censored": int(censored.sum()), "clusters": per_cluster,
        "metrics": metrics, "seconds": round(time.time() - t0, 1),
        "half_life_days": half_life_days, "sklearn": sklearn.__version__,
    }
    return {"version": 3, "model": model, "counts": counts, "meta": meta}


def save(bundle, path, history):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    m = bundle["meta"]
    allm = (m.get("metrics") or {}).get("all") or {}
    history = (history + [{"trained_at": m["trained_at"], "n_samples": m["n_samples"],
                           "model": allm.get("model"), "history": allm.get("history")}])[-HISTORY_KEEP:]
    m["history"] = history
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        pickle.dump(bundle, f)
    os.replace(tmp, path)


def load(path):
    with open(path, "rb") as f:
        bundle = pickle.load(f)
    if bundle.get("version") != 3:
        raise ValueError("old model format")
    return bundle


# ---------------- 预测时的特征：用当前刷新结果拼出和训练时一样的东西 ----------------
def context_of(result):
    """poll_cluster 的结果 -> 预测要用的集群状态"""
    bl = result.get("backlog") or {}
    return {
        "cluster": result["name"],
        "qos": {q["name"]: q for q in result.get("qos", [])},
        "bl": {p["name"]: p for p in bl.get("partitions", [])} if bl.get("ok") else {},
        "parts": (result.get("overview") or {}).get("partitions", {}),
        "load_private": result.get("load_private", False),
    }


def state_features(ctx, qos, partition, gpus, gpu_type):
    q = ctx["qos"].get(qos) or {}
    priv = ctx["load_private"] or q.get("all_running") is None
    f = {
        "cluster": ctx["cluster"], "qos": qos, "partition": partition,
        "qos_all_running": math.nan if priv else num(q.get("all_running")),
        "qos_all_pending": math.nan if priv else num(q.get("all_pending")),
        "qos_mine_running": num(q.get("mine_running")),
        "qos_mine_pending": num(q.get("mine_pending")),
        "part_all_running": num((ctx["parts"].get(partition) or {}).get("all_running")),
        "part_all_pending": num((ctx["parts"].get(partition) or {}).get("all_pending")),
    }
    p = ctx["bl"].get(partition)
    if p:
        f["bl_cpu_min_h"], f["bl_cpu_max_h"] = num(p.get("cpu_min")), num(p.get("cpu_max"))
        if gpus:
            gs = [g for g in p.get("gpu", []) if not gpu_type or g["type"] == gpu_type]
            if gs:
                g = min(gs, key=lambda g: g["max"])
                f["bl_gpu_min_h"], f["bl_gpu_max_h"] = num(g["min"]), num(g["max"])
    return f


def job_samples(j, ctx, now):
    """自己的一个排队 job -> 每个候选分区一个样本。
    还在等 dependency 的：按"刚开始排队"算（已等 0），开始时间另外加上等前置 job 的时间"""
    gtype, gcount = backlog.parse_gpu(j.get("tres", ""))
    parts = [p for p in (j.get("partition") or "").split(",") if p] or [""]
    dep = waiting_on_dependency(j)
    ref = parse_ts(j.get("eligible") or j.get("submit"))
    submit = parse_ts(j.get("submit"))
    est = parse_ts(j.get("start"))  # Slurm 的预计开始时间（等 dependency 时没有）
    base = {
        "reason": "" if dep else norm_reason(j.get("reason")),
        "cpus": num(j.get("cpus")), "gpus": float(gcount or 0), "nodes": num(j.get("nnodes")),
        "time_limit_h": backlog.parse_duration(j["time_limit"]) if j.get("time_limit") else math.nan,
        "mem_gb": math.nan, "n_partitions": len(parts),
        "waited_h": 0.0 if dep or not ref else max((now - ref).total_seconds() / 3600, 0),
        "has_dep": 1.0 if j.get("deps") else 0.0,
        # 还在等依赖的：至少已经等了 现在 − 提交（真正的值要到依赖满足才知道）
        "dep_wait_h": 0.0 if not j.get("deps") or not submit else max(
            ((now if dep else (ref or now)) - submit).total_seconds() / 3600, 0),
        "slurm_remaining_h": (est - now).total_seconds() / 3600 if est and not dep else math.nan,
    }
    return [{**base, **state_features(ctx, j.get("qos", ""), p, gcount, gtype)} for p in parts]


def dependency_ready(jid, jobs, waits, now, memo, stack=()):
    """job 什么时候可以开始排队：(三个分位数的时间戳, 是否全部算得出)。
    afterany / afterok / afternotok / aftercorr / SGE hold_jid：等前置 job 结束；after：等它开始。
    前置 job 结束时间按"开始时间 + 时限"算（上限：一般会早一点结束）"""
    if jid in memo:
        return memo[jid]
    j = jobs.get(jid) or {}
    t = now.timestamp()
    ready, known = [t, t, t], True
    for d in j.get("deps", []):
        pid = d.get("id")
        if not pid or d.get("type") == "singleton" or d.get("status") in ("fulfilled",):
            continue
        p = jobs.get(pid)
        if not p or (p["state"] not in PENDING and p["state"] not in ACTIVE):
            continue  # 已经结束（或者不在列表里）：不用等
        limit = backlog.parse_duration(p["time_limit"]) * 3600 if p.get("time_limit") else None
        if p["state"] in ACTIVE:
            st = parse_ts(p.get("start"))
            starts = [st.timestamp()] * 3 if st else [t] * 3
        elif pid in stack:  # 循环依赖，不管
            continue
        else:
            starts = job_start(pid, jobs, waits, now, memo, stack + (jid,))
            if starts is None:
                known = False
                continue
        if d.get("type") == "after":
            cand = starts
        elif limit is None:
            known = False
            continue
        else:
            cand = [s + limit for s in starts]
        ready = [max(a, b) for a, b in zip(ready, cand)]
    memo[jid] = (ready, known)
    return memo[jid]


def job_start(jid, jobs, waits, now, memo, stack=()):
    """排队 job 预计什么时候开始：[q20, q50, q80] 时间戳（算不出返回 None）"""
    w = waits.get(jid)
    if w is None:
        return None
    ready, _ = dependency_ready(jid, jobs, waits, now, memo, stack)
    return [r + h * 3600 for r, h in zip(ready, w)]


def iso(ts):
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%dT%H:%M:%S")


class Predictor:
    """app.py 里一个实例：加载 / 训练 / 预测。训练在自己的线程里跑，不占 SSH 队列"""

    def __init__(self, path, half_life_days=14):
        self.path = path
        self.half_life_days = half_life_days
        self.bundle = None
        self.error = None  # 加载 / 训练出错（给前端看）
        self.training = False
        self.lock = threading.Lock()
        try:
            import sklearn  # noqa: F401
            self.available = True
        except ImportError:
            self.available = False
        if self.available and os.path.exists(path):
            try:
                self.bundle = load(path)
            except Exception as e:  # noqa: BLE001  比如 sklearn 版本变了
                self.error = L(f"模型文件读不了（{e}），请重新训练", f"Cannot load model ({e}); please retrain")

    def status(self):
        return {"available": self.available, "training": self.training, "error": self.error,
                "meta": self.bundle["meta"] if self.bundle else None}

    def claim(self):
        """开始训练前先占位（在启动线程之前调用，这样状态马上就是"训练中"）。False = 已经在训练了"""
        with self.lock:
            if self.training:
                return False
            self.training = True
            return True

    def train(self, rows, snaps):
        """同步训练（调用方先 claim()，再放到线程里跑）。成功返回 True"""
        try:
            bundle = train(rows, snaps, self.half_life_days)
            history = (self.bundle or {}).get("meta", {}).get("history", [])
            save(bundle, self.path, history)
            self.bundle, self.error = bundle, None
            return True
        except Exception as e:  # noqa: BLE001
            self.error = e.args[0] if isinstance(e, ValueError) and isinstance(e.args[0], dict) \
                else L(f"训练失败：{e}", f"Training failed: {e}")
            return False
        finally:
            self.training = False

    def n_seen(self, cluster, qos, partition):
        return self.bundle["counts"].get(f"{cluster}|{qos}|{partition}", 0) if self.bundle else 0

    def annotate(self, result, now=None):
        """给一个集群结果里所有排队中的 job 加 ml_est：
        {"wait": [q20, q50, q80] 小时（从可以排队起）, "start": [三个时间]（还要等 dependency 的加上
         前置 job 的时间）, "dep_ready": 预计可以排队的时间 / None, "dep_known": 前置 job 的时间算不算得出,
         "partition", "n_seen", "model": cluster/global, "at"}"""
        jobs = result.get("jobs", {})
        for j in jobs.values():
            j.pop("ml_est", None)
        if not self.bundle or not result.get("jobs"):
            return
        now = now or datetime.now()
        ctx = context_of(result)
        todo = [(jid, job_samples(j, ctx, now)) for jid, j in jobs.items()
                if j["state"] in PENDING and j.get("live") and "[" not in jid and not j.get("never_run")]
        if not todo:
            return
        model = self.bundle["model"]
        P = model.predict([s for _, ss in todo for s in ss])
        waits, best_part, k = {}, {}, 0
        for jid, ss in todo:
            rows = P[k:k + len(ss)]
            k += len(ss)
            b = int(rows[:, 1].argmin())  # 好几个候选分区：哪个快算哪个
            waits[jid] = [float(x) for x in rows[b]]
            best_part[jid] = ss[b]["partition"]
        memo = {}
        for jid, _ in todo:
            j = jobs[jid]
            start = job_start(jid, jobs, waits, now, memo)
            ready, known = memo.get(jid, ([now.timestamp()] * 3, True))
            dep = waiting_on_dependency(j)
            j["ml_est"] = {
                "wait": [fmt_q(x) for x in waits[jid]],
                "start": [iso(x) for x in start] if start else None,
                "dep_ready": iso(ready[1]) if dep and known else None,
                "dep_known": known, "dep": dep,
                "partition": best_part[jid], "model": model.which(result["name"]),
                "n_seen": self.n_seen(result["name"], j.get("qos", ""), best_part[jid]),
                "at": now.strftime("%Y-%m-%dT%H:%M:%S"),
            }

    def suggest(self, results, req, now=None):
        """新 job 交到哪里最快。req: {cpus, gpus, gpu_type, nodes, time_h, cluster}
        results: {集群名: poll_cluster 的结果}。返回按 q50 排好的候选（数据太少的排后面）"""
        now = now or datetime.now()
        cpus = num(req.get("cpus"))
        cpus = 1.0 if math.isnan(cpus) else cpus
        gpus = num(req.get("gpus"))
        gpus = 0.0 if math.isnan(gpus) else gpus
        nodes = num(req.get("nodes"))
        nodes = 1.0 if math.isnan(nodes) else nodes
        time_h = num(req.get("time_h"))
        gtype = (req.get("gpu_type") or "").strip()
        cands, samples = [], []
        for name, r in results.items():
            if req.get("cluster") and name != req["cluster"]:
                continue
            ov = r.get("overview") or {}
            ctx = context_of(r)
            limits = {q["name"]: q for q in r.get("qos", [])}
            groups = ov.get("groups")
            if not groups:  # 旧缓存没有 QoS → 分区的树：用你的 job 里见过的组合
                seen = {}
                for j in r.get("jobs", {}).values():
                    for p in (j.get("partition") or "").split(","):
                        if p and j.get("qos"):
                            seen.setdefault(j["qos"], {})[p] = {"name": p}
                groups = [{"qos": q, "children": list(ps.values())} for q, ps in seen.items()]
            for g in groups:
                qos = g["qos"]
                for ch in g["children"]:
                    p = ch["name"]
                    if ch.get("hidden") == "config":  # config.yaml 里 hide_partitions 写了：不考虑
                        continue
                    info = ov.get("partitions", {}).get(p, {})
                    bl = ctx["bl"].get(p)
                    if unfit(info, bl, p, cpus, gpus, nodes, time_h, limits.get(qos, {}).get("max_wall")):
                        continue
                    samples.append({"reason": "", "cpus": cpus, "gpus": gpus, "nodes": nodes,
                                    "time_limit_h": time_h, "mem_gb": math.nan, "n_partitions": 1,
                                    "waited_h": 0.0, "has_dep": 0.0, "dep_wait_h": 0.0,
                                    "slurm_remaining_h": math.nan,
                                    **state_features(ctx, qos, p, gpus, gtype)})
                    cands.append({"cluster": name, "qos": qos, "partition": p,
                                  "n_seen": self.n_seen(name, qos, p),
                                  "model": self.bundle["model"].which(name),
                                  "stale": bool(r.get("stale_since")),
                                  "all_pending": ch.get("all_pending"),
                                  "mine_running": ch.get("mine_running", 0),
                                  "mine_pending": ch.get("mine_pending", 0),
                                  "mine_dep": ch.get("mine_dep", 0),
                                  "backlog": [bl["cpu_min"], bl["cpu_max"]] if bl else None})
        if not samples:
            return []
        P = self.bundle["model"].predict(samples)
        for c, row in zip(cands, P):
            c["wait"] = [fmt_q(float(x)) for x in row]
        cands.sort(key=lambda c: (c["n_seen"] < FEW_SEEN, c["wait"][1]))
        return cands


def unfit(info, bl, name, cpus, gpus, nodes, time_h, max_wall):
    """这个分区放不下这个 job 的原因（None = 可以）"""
    if info.get("avail") and info["avail"] not in ("up", "running"):
        return "down"
    has_gpu = bool(bl and bl.get("gpus")) or "gpu" in name.lower()
    if gpus > 0 and not has_gpu:
        return "no gpu"
    if gpus == 0 and has_gpu:
        return "gpu partition"
    lims = [backlog.parse_duration(x) for x in (info.get("time_limit"), max_wall)
            if x and x not in ("infinite", "UNLIMITED")]
    if not math.isnan(time_h) and any(time_h > lim + 1e-6 for lim in lims):
        return "time limit"
    tot = (info.get("cpus") or {}).get("total")
    if tot and cpus > tot:
        return "too many cpus"
    tn = (info.get("nodes") or {}).get("total")
    if tn and nodes > tn:
        return "too many nodes"
    return None


if __name__ == "__main__":  # python predictor.py：用 waitlog/ 训练并打印回测结果（不保存）
    import json
    import sys
    d = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "waitlog")
    b = train(load_rows(os.path.join(d, "jobs.csv")), Snapshots(d))
    print(json.dumps(b["meta"], indent=1, ensure_ascii=False, default=str))
