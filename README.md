# HPC Job Dashboard

[English](#english) · [中文](#中文)

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

### Run as a systemd service (start on boot)

`python3 install.py` offers this as its last step (it runs the `sudo` commands and asks for
your password); `python3 install.py --uninstall` removes it.

The service is called `hpc-dashboard` and runs as your own user (using your
`~/.ssh/config` and keys; a system service can't reach your desktop ssh-agent, so the keys
must not have a passphrase). Logs: `journalctl -u hpc-dashboard -f`. After changing code or
config.yaml: `sudo systemctl restart hpc-dashboard`. Stop any manually started
`python app.py` first, or the port will clash.

### The page

**QoS panel** (one card per cluster)
- Mine = your running / pending jobs in this QoS
- Cluster = running / pending jobs of all users in this QoS; a long queue means it's busy.
  Some clusters (e.g. with PrivateData) only show your own jobs in squeue; this column
  then says "hidden"
- MaxJobs / MaxSubmit / MaxWall / MaxTRES/user = per-user limits from sacctmgr; reaching
  the running or submit cap is flagged as "run cap" / "submit cap"
- Click a row to filter the job table to that QoS, click again to clear
- The QoS list is discovered automatically (sacctmgr assoc); you can also list them under
  each cluster in config.yaml (`qos:`), with optional notes

**Queue backlog** (one card per cluster)
- Per partition: cores, GPUs, CPU backlog, GPU backlog per GPU type. Backlog = sum of
  (remaining time × cores/GPUs) over all jobs ÷ total cores/GPUs, i.e. "how long to clear
  the current work if it packed perfectly"
- Range Local – Global: the first only counts this partition, the second includes
  partitions sharing the same nodes
- GPU pools: how many cards of each type, shared by which partitions
- Line cutters: other users' jobs submitted after your earliest pending job in the same
  partition but already running (usually via backfill)
- Clusters that hide other users' jobs (bluebear) can't be estimated; a one-line note says so

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

Turn features off per cluster with `disable: [cluster_load, backlog]`, e.g. for a cluster
that hides other users' jobs (this is also auto-detected: if the cluster-wide counts equal
your own, they are shown as hidden). SGE has no "eligible" time; it is taken as the end of
the last `-hold_jid` predecessor. Adding PBS means writing one more subclass and
registering it in `schedulers/__init__.py`.

### Common tweaks

- **Only some clusters**: set `enabled: false` (or delete) the others in `config.yaml`.
- **Automatic refresh**: `auto_refresh_minutes: 5` (SSH has a cost, don't go too low).
- **More files**: add patterns to `inspect_files`, e.g. `OSZICAR`, `slurm-{id}.out`.
- **Dependency history incomplete**: increase `history_hours` (sacct range); too large
  makes each refresh slower.
- **View from phone / other machines**: set `host` in `config.yaml` to `0.0.0.0` and use
  your workstation's LAN IP. Anyone on that network can then see your job names, paths
  and output files; restrict access with your firewall.
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

### 装成 systemd 服务（开机自启）

`python3 install.py` 最后一步会问要不要装（会执行 `sudo` 命令、提示输密码）；
`python3 install.py --uninstall` 删除服务。

服务名 `hpc-dashboard`，以你自己的用户身份运行（用你的 `~/.ssh/config` 和 key；系统服务
拿不到桌面的 ssh-agent，所以 key 不能设密码）。看日志 `journalctl -u hpc-dashboard -f`，
改了代码或 config.yaml 之后 `sudo systemctl restart hpc-dashboard`。装之前先把手动跑的
`python app.py` 停掉，不然端口会冲突。

### 界面说明

**QoS 面板**（每个集群一张卡片）
- 我的 = 你在这个 QoS 上 运行 / 排队 的 job 数
- 全集群 = 所有用户在这个 QoS 上 运行 / 排队 的数量，排队多说明挤。有的集群
  （比如设置了 PrivateData）squeue 只能看到自己的 job，这一列会显示"隐藏"
- MaxJobs / MaxSubmit / MaxWall / MaxTRES/人 = sacctmgr 里的每人限额；运行数或提交
  数到顶了会标"运行已满"/"提交已满"
- 点一行 = 下面的 job 表只看这个 QoS，再点一次取消
- QoS 列表默认自动查（sacctmgr assoc），也可以在 config.yaml 里每个集群下面写
  `qos:` 手动指定，还能加备注

**排队积压**（每个集群一张卡片）
- 每个分区：总核数、总卡数、CPU 积压、各型号 GPU 积压。积压 = 把分区里所有 job 的
  （剩余时长 × 核数/卡数）加起来 ÷ 总核数/卡数，即"完美排满时清空现有的活要多久"
- 范围 Local – Global：前者只算本分区，后者把共享同一批节点的分区都算上
- GPU 池：每种卡一共几张，被哪些分区共用
- 插队：比你在同分区最早排队的 job 晚提交、却已经在跑的别人的 job（通常靠 backfill）
- 看不到其他用户 job 的集群（bluebear）没法估计，会写一行说明

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

单个集群可以用 `disable: [cluster_load, backlog]` 关掉某些功能，比如看不到别人 job 的集群
（这个也会自动识别：全集群数字和你自己的一样时显示为"隐藏"）。SGE 没有 eligible
（可以开始排队的）时间，用 `-hold_jid` 依赖里最后一个结束的时间来补。以后要加 PBS，
就再写一个子类，登记到 `schedulers/__init__.py`。

### 常见调整

- **只想看某几个集群**：在 `config.yaml` 里把不需要的集群 `enabled: false` 或删掉。
- **想自动刷新**：`auto_refresh_minutes: 5`（SSH 有开销，别设太小）。
- **想多看几个文件**：在 `inspect_files` 里加模式，比如 `OSZICAR`、`slurm-{id}.out`。
- **依赖链历史看不全**：加大 `history_hours`（sacct 查询范围），但太大会让每次
  刷新变慢。
- **想在手机/同事电脑上也能看**：把 `config.yaml` 里 `host` 改成 `0.0.0.0`，
  然后用你工作电脑的局域网 IP 访问。注意这样局域网内的人都能看到你的作业名字
  等信息，公司网络环境要自己权衡安全性。
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
