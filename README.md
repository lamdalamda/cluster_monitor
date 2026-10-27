# HPC Job Dashboard

[English](#english) · [中文](#中文)

> **For the impatient, by the impatient.**
>
> Your jobs are already the most patient patients in the world: they sit in the queue's
> waiting room for days without complaining. You, on the other hand, have run `squeue` 37
> times since lunch, on four clusters, in four terminals, and one of them wants a 2FA code.
>
> This repo is the triage board. One page shows every job on every cluster: what's running,
> what's waiting and why, which partition is quiet, which job will *never* start (sorry, the
> dependency it was waiting for died), and a small model guessing when the rest will start
> and where your next job would start soonest. Then you can close the terminals and go be
> impatient about something else.
>
> No admin rights, no agents on the cluster, no web API: just your existing `ssh` keys.
> Setup takes about as long as the wait for one `squeue`.

> **你看，你又急。**
>
> 交完 job 才三分钟，你已经 `squeue` 了八遍；一个集群不够，四个集群轮流 ssh，其中一个还要
> 先输 2FA；排了一晚上没动，第二天才发现是依赖的 job 早就挂了……
>
> 我觉得你有点急了。
>
> 这个 repo 就是给急的人准备的：一个网页把所有集群上的 job 摆在一起——哪些在跑、哪些在排、
> 为什么在排、哪个分区比较空、哪个 job **永远不会开始**（赶紧删了重交），再用一个小模型猜猜
> 剩下的什么时候开始、下一个 job 往哪交最快。看完就可以把终端关了，安心地去急别的事。
>
> 不用管理员权限，不用在集群上装任何东西，不走 web API，用的就是你现有的 `ssh` key。
> 装好它花的时间，比你等一次 `squeue` 还短。

### TL;DR

```bash
git clone https://github.com/lamdalamda/cluster_monitor.git
cd cluster_monitor
python3 install.py   # 回答几个问题，一路回车也行 / answer a few questions (Enter = default)
python3 app.py       # 或者让 install.py 装成 systemd 服务 / or let install.py set up a systemd service
```

然后打开 / then open <http://127.0.0.1:8787>，点"立即刷新" / click "Refresh"。
想要排队预测：在跑 dashboard 的 Python 里 `pip install scikit-learn`，再到页面上点"训练 / 更新模型"。
For wait prediction: `pip install scikit-learn` in the Python that runs the dashboard, then
click "Train / update model" on the page.

前提 / Prerequisite: `ssh <集群 cluster>` 不用输密码就能登录（ssh key + `~/.ssh/config`）。
Slurm 和 SGE 都支持。

## English

A small web page you run on your own workstation to see, as tables, your jobs on several
Slurm and SGE (Son of Grid Engine) clusters: their state, which nodes they run on, and how many you have running /
pending in each QoS, so you can decide where to submit the next job.

### How it works

**The page is decoupled from SSH**: every remote operation goes into one background task
queue and runs one after another; a progress bar at the top shows what the backend is
doing. Opening the page shows the last data immediately (stored in `.dashboard_cache.json`,
kept across restarts of app.py), without waiting for SSH. **No automatic refresh by
default**: data is fetched when you click "Refresh"; set `auto_refresh_minutes` in
config.yaml if you want it automatic.

Each refresh runs **one SSH session per cluster** (clusters one after another) with:
- `squeue --me -o ...`: your current pending/running jobs (QoS, dependencies, estimated start)
- `sacct -X --starttime=now-Nhours ...`: jobs that finished in the last N hours
- `squeue -t RUNNING,PENDING -o '%q|%T'` (all users): cluster-wide running/pending per QoS
- `sacctmgr show assoc / show qos`: which QoS you can use and their limits
  (rarely changes; queried every 30 minutes by default)
- `squeue --all` + `sinfo`: the queue backlog estimate (logic from godot.py used on sulis,
  see `backlog.py`); heavier, fetched at most every 5 minutes by default

**This is plain `ssh <host> <command>`, not an API/REST connection**, so:
- your machine must be able to log in to each login node with an SSH key (key + agent,
  or a key without passphrase)
- clusters with 2FA (sulis) work while the 2FA is still valid. The program **never** types
  a code for you: as soon as the server actually prompts for input, `ssh_askpass_abort.sh`
  kills ssh (nothing is sent, so no failed attempts pile up), the page shows "2FA needed"
  and automatic retries pause for 10 minutes. Log in once with `ssh sulis` in a terminal,
  then click "Refresh"
- Host aliases, ProxyJump, jump hosts etc. from your `~/.ssh/config` are used

### Install & run

```bash
cd cluster_monitor
python3 install.py
```

`install.py` is interactive (standard library only, any python3 works):
1. picks the Python that will run the dashboard and checks / installs flask + pyyaml
2. optionally reads `~/.ssh/config`, optionally SSHes to each Host to test which have Slurm
   (only `command -v sbatch`; hosts asking for 2FA/password are skipped, nothing is typed),
   and lets you pick the clusters
3. asks for the listen address, port, auto refresh, history hours, folder size alert and
   whether to collect wait-time data, then writes `config.yaml` from `config.example.yaml`
   (comments kept; an existing config.yaml is backed up first)
4. optionally installs the systemd service (see below)

`config.yaml` is your own file and not tracked by git (the repo ships `config.example.yaml`),
so pulling updates never conflicts with your settings. You can also copy
`config.example.yaml` to `config.yaml` and edit it by hand. Run manually with
`python app.py`, then open `http://127.0.0.1:8787` (port set in config.yaml).

### Wait-time data (waitlog/)

Every refresh writes each job's requested resources, the cluster state when it was first
seen pending (QoS queue counts, backlog estimate, Slurm's own estimated start, reason,
priority) and how long it finally waited into `waitlog/jobs.csv`, and appends QoS /
partition state snapshots. This is for building a wait-time prediction model later; the
columns are described in [waitlog/README.md](waitlog/README.md).

The "Queue history" section at the bottom of the page (collapsed by default) reads this
file: per cluster × QoS, how many jobs started and their median / P25–P75 / P90 / max wait
(start − eligible), plus a searchable list of past jobs with submit / start / end times.

- **Size limit**: when the `waitlog/` folder grows past `waitlog_max_mb` (default 500 MB)
  nothing more is written to it and the page shows an alert.
- **Backups**: when you click "Refresh" and the newest backup is older than
  `waitlog_backup_days` (default 7) days, `waitlog/` is packed into
  `waitlog_backups/waitlog-YYYYmmdd-HHMMSS.tar.gz`; only the newest `waitlog_backup_keep`
  (default 10) are kept. Automatic refreshes never make a backup, and no new backup is made
  if nothing changed since the last one.

### Wait prediction (predictor.py)

The "Wait prediction" section (collapsed by default, under the QoS tree) trains a small
model on the waitlog data when you click **Train / update model** (a few seconds; needs
scikit-learn, saved to `models/wait_model.pkl` and loaded on start). After that:

- every pending job gets a purple **model** start estimate in the Jobs time column (hover
  for the q20–q80 range). Jobs still waiting on dependencies: start = predecessors' end
  (start + time limit, so conservative) + the predicted queue wait;
- "Where to submit": enter cores / GPUs / nodes / hours, and every QoS + partition that
  fits is ranked by predicted wait, using the latest refresh's queue state.

Model: scikit-learn HistGradientBoosting with quantile loss (q20 / q50 / q80), one per
cluster (global fallback for clusters with little data); QoS / partition are features.
Inputs: requested resources and the queue state when the job became eligible (QoS and
partition running / pending, backlog estimate, Slurm's estimate and reason, time already
waited, whether / how long it waited on dependencies). Waits are always counted from
eligible. Jobs submitted together count as one sample; older data is down-weighted
(`wait_model_half_life_days`). Each training run backtests over time against "historical
quantiles" and uses those instead on clusters where they did better; the backtest table is
shown in the page. Predictions with under 3 batches of history are greyed out ("little
data"). Refreshing more often (`auto_refresh_minutes: 15`) gives the model much better data.
`python predictor.py` trains and prints the backtest without saving.

### Run as a systemd service (start on boot)

`python3 install.py` offers this as its last step (it runs the `sudo` commands and asks for
your password); `python3 install.py --uninstall` removes it.

The service is called `hpc-dashboard` and runs as your own user (using your
`~/.ssh/config` and keys; a system service can't reach your desktop ssh-agent, so the keys
must not have a passphrase). Logs: `journalctl -u hpc-dashboard -f`. After changing code or
config.yaml: `sudo systemctl restart hpc-dashboard`. Stop any manually started
`python app.py` first, or the port will clash.

### The page

**QoS / partitions / backlog** (one card per cluster, a tree: QoS → the partitions it can use)
- In Slurm a **partition** is a set of nodes (where a job runs) and a **QoS** is a set of
  rules (per-user caps, max wall time, priority). A job picks one QoS and one or more
  partitions, so they are many-to-many, not nested. Each QoS row lists the partitions it
  can use: from the partition's `AllowQos` / `DenyQos` (`scontrol show partition`); with
  `AllowQos=ALL`, from combinations actually seen (all users' jobs + your own, including
  finished ones), or every partition if you only have one QoS. Partitions under no QoS go
  to "Other partitions" (collapsed). Override per cluster with
  `qos_partitions: {bbgpu: [icelake-gpua100-shared]}`
- QoS row: Mine / Cluster = running / pending in this QoS (Cluster says "hidden" on
  clusters that only show your own jobs), MaxWall, and the sacctmgr limits (MaxJobs /
  MaxSubmit / MaxTRES per user); reaching the running / submit cap is flagged
- Partition row (click ▶ on the QoS): Mine / Cluster for this QoS in this partition, nodes
  and CPUs allocated / idle / total (`sinfo`; only totals from `scontrol` where sinfo is
  forbidden), CPU / GPU backlog, time limit, and the partition's own QoS with its limits
  (e.g. sulis' `part_compute`), which apply to every job in it
- Backlog = sum of (remaining time × cores/GPUs) over all jobs ÷ total cores/GPUs, i.e.
  "how long to clear the current work if it packed perfectly"; range Local – Global (this
  partition only – including partitions sharing the same nodes). Under the table: GPU pools
  and "line cutters" (other users' jobs submitted after your earliest pending job in the
  same partition but already running, usually via backfill). Clusters that hide other
  users' jobs (bluebear) can't be estimated; the footer says so
- Click a QoS row to filter the job table to that QoS, a partition row to filter by
  partition (click the chip next to "Jobs" to clear). A pending job listing several
  partitions is counted in each. The QoS list is discovered automatically (sacctmgr assoc);
  you can also list them under each cluster in config.yaml (`qos:`), with optional notes

**Job table**
- Jobs with the **same name** on a cluster (usually one dependency chain, or repeated
  submissions) share one row; `×N` is the count, click ▶ to see each one. The collapsed
  row shows the "current" job: running > next pending > last one; the small tags count
  the states in the group (R running, PD pending, CD completed, F failed, CA cancelled,
  TO timeout…)
- Order: groups with running jobs first, then pending, then finished
- The "Dependencies" column shows e.g. `afterok #12345`; the dot is the state colour of
  the job depended on, click it to jump there
- For pending jobs the "Time" column shows Slurm's estimated start (`est.`) and a wait
  range from the backlog (`backlog`, bottleneck resource; hover for CPU/GPU details). Jobs
  stuck on limits like `AssocGrpJobsLimit` / `QOSMaxGRESPerUser` show "limit-blocked":
  your own (or your group's) quota is full, regardless of how busy the cluster is
- If a cluster is unreachable the last data stays visible (greyed out) and the card says
  how old it is
- Click a Job ID to copy it (handy for `scancel` or `--dependency=afterok:ID`)
- Hover a name for full details (raw dependency string, submit/start/end times etc.)
- Filter by cluster / QoS / state and search at the top; expanded rows and filters are
  remembered in the browser
- "Refresh" queues a refresh in the background and returns immediately; the data updates
  when the progress bar finishes
- Switch between English / 中文 at the top right (remembered in the browser)
- **Expanding** a group (or a single job) makes the backend read each job's working folder
  (squeue `%Z` / sacct `WorkDir`): `ls -lt`, then for each pattern in `inspect_files`
  (default `vasp.out`, `Slab*.log`, slurm output `*{id}*.out*`) the newest matching file is
  tailed + grepped. Each job gets an extra line: path (click to copy), number of files,
  vasp.out ionic step / F / dE / electronic step, converged or not, last error; "Details"
  shows the full `ls -lt` and tail. Results are cached; "Re-read" reads again. A folder
  shared by several jobs is read once (each job only adds its own slurm output) and the
  other jobs in that folder are listed
- **Alerts** at the top:
  - same folder: several jobs (different job ids; tasks of one array don't count) RUNNING
    in the same folder at once, usually a wrong dependency. Based on squeue only, no need
    to expand
  - folder too large: `du -sk` of a read folder exceeds `dir_size_warn_gb` (default 30G)
  - too many files: a read folder has more than `dir_files_warn` entries (default 50)
  - slow LOOP: the folder has an `OUTCAR` (only its last 5 MB is grepped for `LOOP`); the
    latest electronic step's cpu / real time is shown next to the ionic step, and a still
    running job is flagged when real > 1.1 × cpu **and** real > cpu + 5 s (waiting on
    communication / IO), or real > 1000 s. Thresholds: `loop_warn_ratio`,
    `loop_warn_extra_seconds`, `loop_warn_real_seconds`
  - click a job id to jump to it, click a path to copy it

### Schedulers (Slurm / SGE)

Each cluster in `config.yaml` can set `scheduler: slurm` (default) or `scheduler: sge`.
Every scheduler is a subclass of `schedulers/base.py:Scheduler` (`schedulers/slurm.py`,
`schedulers/sge.py`) that says which commands to run, how to parse them, and which
**features** it supports; the page only shows what a cluster supports:

| feature | Slurm | SGE |
|---|---|---|
| your jobs | squeue | `qstat -u $USER -r -xml` + `qstat -j` (project, folder, `-hold_jid`) |
| finished jobs (`history`) | sacct | tail of the `accounting` file (`accounting_file`, `accounting_tail_mb`) |
| cluster-wide counts (`cluster_load`) | squeue all users | `qstat -u '*' -ext`, per project |
| QoS list & limits (`qos_info`) | sacctmgr | — (QoS panel groups by project, no limit columns) |
| backlog estimate (`backlog`) | squeue + sinfo | — |
| scheduler's estimated start (`est_start`) | squeue | — |
| partition nodes / CPUs, AllowQos (`partition_info`) | sinfo + scontrol | — (partition table groups your jobs by PE) |

Turn features off per cluster with `disable: [cluster_load, backlog]`, e.g. for a cluster
that hides other users' jobs (this is also auto-detected: if the cluster-wide counts equal
your own, they are shown as hidden). SGE has no "eligible" time; it is taken as the end of
the last `-hold_jid` predecessor. Adding PBS means writing one more subclass and
registering it in `schedulers/__init__.py`.

### Common tweaks

- **Only some clusters**: set `enabled: false` (or delete) the others in `config.yaml`.
- **Automatic refresh**: the dropdown next to "Refresh" (off / 6 / 12 / 24 h, counted from
  the last refresh, manual ones included; saved in `.dashboard_settings.json`, which wins over
  `auto_refresh_minutes` in config.yaml).
- **Hide partitions**: partitions with no jobs are folded into "Other partitions"
  automatically; list more per cluster in `hide_partitions` (wildcards ok).
- **"Won't start" alerts**: pending jobs whose reason matches `never_run_reasons`
  (DependencyNeverSatisfied, PartitionTimeLimit, QOSMax*PerJob*, held, SGE Eqw … see
  config.example.yaml) get an alert at the top and no start estimate.
- **Waiting on dependencies**: in "mine" counts, jobs waiting on dependencies aren't counted
  as pending; they're shown in brackets after running, e.g. `2 (+5) / 98`.
- **More files**: add patterns to `inspect_files`, e.g. `OSZICAR`, `slurm-{id}.out`.
- **Dependency history incomplete**: increase `history_hours` (sacct range); too large
  makes each refresh slower.
- **View from phone / other machines**: set `host` in `config.yaml` to `0.0.0.0` and use
  your workstation's LAN IP. Anyone on that network can then see your job names, paths
  and output files; restrict who can open it with `allow_from` (IPs / subnets, others get
  403), e.g. `allow_from: [127.0.0.1, "::1", 192.168.192.0/24]`, or with your firewall.
- **SGE output files**: SGE writes `jobname.o12345`; give that cluster its own
  `inspect_files` with `"*.o{id}"`.

### Known limitations

- Dependencies are only resolved within one cluster (Slurm has no cross-cluster dependencies).
- Slurm drops satisfied dependencies from squeue's `%E` and sacct doesn't record them, so a
  started/finished job no longer shows what it depended on; this is why jobs are grouped by name.
- Grouping is by name only: two jobs with different names aren't grouped even if one depends
  on the other (the dependency column still shows and links it); conversely, unrelated jobs
  with the same name are grouped.
- Array job (`123_4`) dependencies are matched by the integer id; complex array dependency
  expressions (on some indices only) aren't resolved per index.
- No authentication. It listens on 127.0.0.1 by default; if you open it to 0.0.0.0, put it
  behind a firewall rule, an SSH tunnel or a reverse proxy with authentication.

## 中文

在你自己的工作电脑上跑一个小网页，用表格看多个 Slurm / SGE 集群上你的作业状态、
跑在哪个节点、每个 QoS 上你跑了/排了多少，方便决定下一个 job 往哪交。

### 原理

**网页和 SSH 是解耦的**：所有远程操作都放进一个后台任务队列，一个接一个执行，网页
上方的进度条显示后台正在干什么。打开网页时直接显示上一次的数据（存在
`.dashboard_cache.json`，重启 app.py 也还在），不用等 SSH。**默认不自动刷新**，
点"立即刷新"才去拉；想自动刷新就在 config.yaml 里设 `auto_refresh_minutes`。

每次刷新，对每个集群**一次 SSH**（集群之间依次进行）跑这几条命令：
- `squeue --me -o ...` 拿当前排队/运行中的作业（含 QoS、依赖、预计开始时间）
- `sacct -X --starttime=now-Nhours ...` 拿最近 N 小时内已结束的作业
- `squeue -t RUNNING,PENDING -o '%q|%T'`（所有用户）统计每个 QoS 全集群的运行/排队数
- `sacctmgr show assoc / show qos`：你能用哪些 QoS、每个 QoS 的限额
  （这个基本不变，默认 30 分钟才查一次）
- `squeue --all` + `sinfo`：排队积压估计（逻辑来自 sulis 上用的 godot.py，见 `backlog.py`），
  比较重，默认 5 分钟才拉一次

**这不是通过 API/REST 连集群，就是普通的 `ssh <host> <command>`**，所以：
- 需要你本机能用 SSH key 登录各集群登录节点（SSH key + agent，或者 key 没设密码）
- 需要 2FA 的集群（sulis）：2FA 在有效期内就能直接用。程序**不会**替你输验证码——
  服务器一旦真的弹出输入提示，`ssh_askpass_abort.sh` 会立刻把 ssh 杀掉（什么都不发，
  不会累积失败次数），网页上显示"需要 2FA"，并且 10 分钟内不再自动重试。
  这时在终端里 `ssh sulis` 登录一次，再点网页上的"立即刷新"就好
- 会用到你 `~/.ssh/config` 里已经配置好的 Host 别名、ProxyJump、跳板机等设置

### 安装 & 运行

```bash
cd cluster_monitor
python3 install.py
```

`install.py` 是交互式的（只用标准库，任何 python3 都能跑）：
1. 选用哪个 Python 运行 dashboard，检查 / 安装 flask + pyyaml
2. 可以读取 `~/.ssh/config`，可以挨个 ssh 上去测试哪些 Host 有 Slurm（只跑
   `command -v sbatch`；要 2FA/密码的直接跳过，不会替你输入），然后选要监控的集群
3. 问监听地址、端口、自动刷新、历史时长、目录大小报警、要不要收集等待时间数据，
   以 `config.example.yaml` 为模板生成 `config.yaml`（保留注释；已有的会先备份）
4. 可以顺便装成 systemd 服务（见下）

`config.yaml` 是你自己的文件，不进 git（仓库里只有 `config.example.yaml`），所以以后
拉更新不会和你的配置冲突。也可以手动把 `config.example.yaml` 复制成 `config.yaml` 再改。
手动运行 `python app.py`，然后浏览器打开 `http://127.0.0.1:8787`（端口见 config.yaml）。

### 等待时间数据（waitlog/）

每次刷新会把每个 job 的请求资源、第一次看到它排队时的集群状态（QoS 排队数、积压估计、
Slurm 自己的预计开始时间、原因、优先级）和最终等了多久写进 `waitlog/jobs.csv`，
另外追加 QoS / 分区的状态快照。用来以后建一个预测等待时间的模型，列的含义见
[waitlog/README.md](waitlog/README.md)。

页面最下面的"排队历史"（默认收起）读的就是这个文件：按 集群 × QoS 统计已经开始的 job
数和等待时间（开始 − 可以排队的时间）的中位数 / P25–P75 / P90 / 最长，下面列出以前的 job
（提交 / 开始 / 结束时间），可以搜索。

- **大小上限**：`waitlog/` 文件夹超过 `waitlog_max_mb`（默认 500 MB）就不再写入，页面上会提醒。
- **备份**：点"立即刷新"时，如果最新的备份已经超过 `waitlog_backup_days`（默认 7）天，
  就把 `waitlog/` 打包成 `waitlog_backups/waitlog-YYYYmmdd-HHMMSS.tar.gz`，只保留最新的
  `waitlog_backup_keep`（默认 10）份。自动刷新不会备份；上次备份之后没有变化也不会再备份。

### 排队预测（predictor.py）

"排队预测"（在 QoS 树下面，默认收起）里点**训练 / 更新模型**，就用 waitlog 的数据训练一个
小模型（几秒；需要 scikit-learn，存到 `models/wait_model.pkl`，启动时自动加载）。之后：

- Jobs 表"时间"列里每个排队的 job 多一个紫色的**模型**预计开始时间（鼠标悬停看 q20–q80
  范围）。还在等依赖的 job：开始时间 = 前置 job 结束（开始 + 时限，偏保守）+ 预测的排队时间；
- "预测交到哪里"：填核数 / GPU / 节点 / 时长，按最近一次刷新的排队状态，把所有放得下的
  QoS + 分区按预计排队时间排好。

模型：scikit-learn HistGradientBoosting，分位数损失（q20 / q50 / q80），每个集群一组（数据太少的
集群用全局模型），QoS / 分区是特征。输入：申请的资源，以及 job **可以排队那一刻**的状态（QoS 和
分区的运行 / 排队数、积压估计、Slurm 的估计和原因、已经排了多久、有没有 / 等了多久依赖）。
等待一律从 eligible 算起。一起交的一批 job 只算一份，越旧的数据权重越小
（`wait_model_half_life_days`）。每次训练都会按时间回测，和"历史分位数"比，哪个集群历史分位数
更准就用它；回测表在页面上能看到。历史数据不到 3 批的预测显示成灰色（"数据少"）。
刷新越频繁（`auto_refresh_minutes: 15`），数据越好。`python predictor.py` 训练并打印回测（不保存）。

### 装成 systemd 服务（开机自启）

`python3 install.py` 最后一步会问要不要装（会执行 `sudo` 命令、提示输密码）；
`python3 install.py --uninstall` 删除服务。

服务名 `hpc-dashboard`，以你自己的用户身份运行（用你的 `~/.ssh/config` 和 key；系统服务
拿不到桌面的 ssh-agent，所以 key 不能设密码）。看日志 `journalctl -u hpc-dashboard -f`，
改了代码或 config.yaml 之后 `sudo systemctl restart hpc-dashboard`。装之前先把手动跑的
`python app.py` 停掉，不然端口会冲突。

### 界面说明

**QoS / 分区 / 积压**（每个集群一张卡片，树状：QoS → 它能用的分区）
- Slurm 里**分区** = 一批节点（job 在哪跑），**QoS** = 一套规则（每人上限、最长时间、
  优先级）。每个 job 选一个 QoS 加一个或几个分区，两者是多对多，不是包含关系。每个 QoS
  下面挂它能用的分区：按分区的 `AllowQos` / `DenyQos`（`scontrol show partition`）；
  `AllowQos=ALL` 时按实际见过的组合（全集群的 job + 你自己的 job，包括已结束的），你只有
  一个 QoS 时所有分区都挂在它下面。没挂到任何 QoS 下面的放进"其他分区"（默认收起）。
  可以在 config.yaml 这个集群下面手动指定：`qos_partitions: {bbgpu: [icelake-gpua100-shared]}`
- QoS 行：我的 / 全集群 = 这个 QoS 的 运行 / 排队（只能看到自己 job 的集群显示"隐藏"）、
  MaxWall、sacctmgr 里的每人限额（MaxJobs / MaxSubmit / MaxTRES）；运行数或提交数到顶
  会标"运行已满"/"提交已满"
- 分区行（点 QoS 前面的 ▶）：这个 QoS 在这个分区的 我的 / 全集群，节点和 CPU 的
  已用 / 空闲 / 总数（`sinfo`；不让用 sinfo 的集群只有 `scontrol` 给的总数），CPU / GPU
  积压，时限，以及分区自己的 QoS 和限额（比如 sulis 的 `part_compute`，对这个分区里所有
  job 生效）
- 积压 = 把分区里所有 job 的（剩余时长 × 核数/卡数）加起来 ÷ 总核数/卡数，即"完美排满
  时清空现有的活要多久"；范围 Local – Global（只算本分区 – 算上共享同一批节点的分区）。
  表格下面是 GPU 池和"插队"（比你在同分区最早排队的 job 晚提交、却已经在跑的别人的 job，
  通常靠 backfill）。看不到其他用户 job 的集群（bluebear）没法估计，卡片底部会写一行说明
- 点 QoS 行 = 下面的 job 表只看这个 QoS，点分区行 = 只看这个分区（点 "Jobs" 旁边的标签
  取消）。排队时写了好几个分区的 job 每个分区都算一次。QoS 列表默认自动查（sacctmgr
  assoc），也可以在 config.yaml 里每个集群下面写 `qos:` 手动指定，还能加备注

**Job 表格**
- 同一个集群里**名字相同**的 job（一般就是一条 dependency 链，或者同一批重复提交）
  收成一行，`×N` 是个数，点 ▶ 展开看每一个。折叠行显示的是"当前"那个 job：
  正在跑的 > 下一个排队的 > 最后一个；旁边的小标签是这组里各状态的数量（R 运行、
  PD 排队、CD 完成、F 失败、CA 取消、TO 超时…）
- 排序：有 job 在跑的组在最上面，然后是排队的，然后是已经结束的
- "依赖"列显示 `afterok #12345` 这种，前面的小圆点是被依赖 job 的状态颜色，
  点它会跳到那个 job
- 排队中的 job，"时间"列显示 Slurm 估计的开始时间（`预计`），以及按积压算的等待范围
  （`积压`，取瓶颈资源；鼠标停上去看 CPU/GPU 各自的估计）。如果 job 卡在
  `AssocGrpJobsLimit` / `QOSMaxGRESPerUser` 这类限额上，会显示"卡在限额"——这时是你
  自己（或同组）的额度满了，跟集群忙不忙没关系
- 集群暂时连不上时继续显示上一次拉到的数据（变灰），卡片上写着是几点的旧数据
- 点 Job ID 复制（方便 `scancel` 或者 `--dependency=afterok:ID`）
- 鼠标停在名字上能看到完整信息（原始 dependency 字符串、提交/开始/结束时间等）
- 顶部可以按集群 / QoS / 状态筛选、搜索；展开状态和筛选条件会记在浏览器里
- "立即刷新"把一次刷新放进后台队列，马上返回；进度条走完数据就更新了
- 右上角可以切换中文 / English（记在浏览器里）
- **展开**一组（或单个）job 时，后台会去读每个 job 的工作目录（squeue `%Z` / sacct
  `WorkDir`）：`ls -lt`，再对 `inspect_files` 里每个模式（默认 `vasp.out`、`Slab*.log`、
  slurm 输出文件 `*{id}*.out*`）取最新的匹配文件 tail + grep。每个 job 下面会多一行：
  路径（点击复制）、文件数、vasp.out 的离子步 / F / dE / 电子步、是否收敛、最后一条报错；
  点"详情"看完整的 `ls -lt` 和 tail。读过的会缓存，点"重新读取"才重新读。
  多个 job 共用同一个目录时只读一次（每个 job 只额外找自己的 slurm 输出文件），
  并显示"同目录"还有哪些 job
- 顶部的**提醒**：
  - 同目录同时运行：好几个 job（不同的 job id，同一个 array 的 task 不算）同时 RUNNING
    在同一个目录里，一般是 dependency 没设对。这个只看 squeue，不用展开
  - 目录过大：读目录时 `du -sk` 的结果超过 `dir_size_warn_gb`（默认 30G）
  - 文件过多：读过的目录里文件/子目录超过 `dir_files_warn` 个（默认 50）
  - LOOP 太慢：目录里有 `OUTCAR` 时（只 grep 最后 5 MB 里的 `LOOP`），最后一个电子步的
    cpu / real time 显示在离子步旁边；还在跑的 job 满足 real > 1.1 × cpu **且**
    real > cpu + 5 秒（在等通信 / IO），或者 real > 1000 秒就报警。阈值：
    `loop_warn_ratio`、`loop_warn_extra_seconds`、`loop_warn_real_seconds`
  - 点 job id 跳过去，点路径复制

### 调度系统（Slurm / SGE）

`config.yaml` 里每个集群可以写 `scheduler: slurm`（默认）或 `scheduler: sge`。
每种调度系统是 `schedulers/base.py:Scheduler` 的一个子类（`schedulers/slurm.py`、
`schedulers/sge.py`），负责跑什么命令、怎么解析、支持哪些**功能**；页面只显示这个集群支持的：

| 功能 | Slurm | SGE |
|---|---|---|
| 自己的 job | squeue | `qstat -u $USER -r -xml` + `qstat -j`（project、工作目录、`-hold_jid`） |
| 已结束的 job（`history`） | sacct | tail `accounting` 文件（`accounting_file`、`accounting_tail_mb`） |
| 全集群统计（`cluster_load`） | squeue 所有用户 | `qstat -u '*' -ext`，按 project |
| QoS 和限额（`qos_info`） | sacctmgr | 没有（QoS 面板按 project 分组，不显示限额列） |
| 排队积压（`backlog`） | squeue + sinfo | 没有 |
| 调度器预计开始时间（`est_start`） | squeue | 没有 |
| 分区的节点 / CPU、AllowQos（`partition_info`） | sinfo + scontrol | 没有（分区表按你的 job 的 PE 统计） |

单个集群可以用 `disable: [cluster_load, backlog]` 关掉某些功能，比如看不到别人 job 的集群
（这个也会自动识别：全集群数字和你自己的一样时显示为"隐藏"）。SGE 没有 eligible
（可以开始排队的）时间，用 `-hold_jid` 依赖里最后一个结束的时间来补。以后要加 PBS，
就再写一个子类，登记到 `schedulers/__init__.py`。

### 常见调整

- **只想看某几个集群**：在 `config.yaml` 里把不需要的集群 `enabled: false` 或删掉。
- **自动刷新**："立即刷新"旁边的下拉框（关 / 6 / 12 / 24 小时，从上一次刷新算起，手动的也算；
  存在 `.dashboard_settings.json`，比 config.yaml 里的 `auto_refresh_minutes` 优先）。
- **隐藏分区**：现在没有 job 的分区自动收进"其他分区"；每个集群还可以在 `hide_partitions`
  里写要隐藏的（支持通配符）。
- **"不会开始"告警**：排队原因匹配 `never_run_reasons` 的 job（DependencyNeverSatisfied、
  PartitionTimeLimit、QOSMax*PerJob*、被 hold、SGE 的 Eqw……见 config.example.yaml）在页面顶上告警，
  也不给它预测开始时间。
- **等依赖的 job**："我的"数字里不算排队，放在运行数后面的括号里，比如 `2 (+5) / 98`。
- **想多看几个文件**：在 `inspect_files` 里加模式，比如 `OSZICAR`、`slurm-{id}.out`。
- **依赖链历史看不全**：加大 `history_hours`（sacct 查询范围），但太大会让每次
  刷新变慢。
- **想在手机/同事电脑上也能看**：把 `config.yaml` 里 `host` 改成 `0.0.0.0`，
  然后用你工作电脑的局域网 IP 访问。注意这样局域网内的人都能看到你的作业名字
  等信息。用 `allow_from` 限制谁能打开（IP 或网段，其他地址返回 403），比如
  `allow_from: [127.0.0.1, "::1", 192.168.192.0/24]`。
- **SGE 的输出文件**：SGE 写的是 `job名.o12345`，给这个集群单独写 `inspect_files`，
  加上 `"*.o{id}"`。

### 已知限制

- Dependency 目前只处理同一个集群内部的作业 id（Slurm 本身也不支持跨集群 dependency）。
- Slurm 在依赖满足之后会把它从 squeue 的 `%E` 里去掉，sacct 也不记录 dependency，
  所以已经跑起来/跑完的 job 看不到它当初依赖谁，这也是为什么按名字分组。
- 分组只看名字：名字不同的两个 job 即使有依赖关系也不会收到一起（依赖列里能看到、
  能点过去）；反过来，名字相同但没关系的 job 也会被收到一起。
- Array job（`123_4` 这种）依赖解析是按整数 id 匹配的，正常情况没问题，
  但非常复杂的 array 依赖表达式（比如依赖某个 array 里的部分 index）不会
  精细区分到具体 index。
- 没做鉴权，默认只监听 127.0.0.1，公司电脑上一般够用；如果开 0.0.0.0 建议
  自己加一层反向代理 + 基本认证。
