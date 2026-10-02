# waitlog：排队等待数据

`app.py` 每次刷新成功一个集群，就更新这里的 csv（csv 本身不进 git）。目的是攒数据，
以后建一个"这个 job 大概要排多久"的模型。关掉：config.yaml 里 `waitlog: false`。

## jobs.csv —— 每个 job 一行，每次刷新整体重写

| 列 | 含义 |
|---|---|
| cluster, jobid, name | 集群、job id（array task 是 `123_4`）、名字 |
| partition, qos | 提交时请求的分区（排队时可能是逗号分隔的多个）和 QoS |
| nodes, cpus, gpus, gpu_type, mem | 请求的资源（squeue / sacct ReqTRES） |
| time_limit, time_limit_h | 申请时长（原始字符串 / 小时） |
| dependency | 第一次看到时 squeue 里的依赖字符串 |
| submit, eligible, start, end | 集群本地时间。eligible = 依赖满足、可以开始排队的时间 |
| state | 最新状态 |
| run_partition, nodelist | 实际跑在哪个分区 / 节点（开始后才有） |
| wait_from_submit_h | start − submit（小时） |
| wait_from_eligible_h | start − eligible（小时）。**建模一般用这个**，不含等依赖的时间 |
| censored | 1 = 没观察到开始（还在排，或排队时被取消）。这时 wait_* 是"至少等了这么久"（算到现在 / 取消时间），适合当生存分析里的删失数据 |
| first_seen_at, last_seen_at | 本机第一次 / 最近一次看到这个 job 的时间 |
| seen_pending | 1 = 我们看到过它在排队，下面的 obs_* 列才有值 |
| obs_at, obs_lag_h | 第一次看到它排队的本机时间；离 eligible/submit 已经过了多久（越小越接近"提交时的状态"） |
| obs_reason, obs_priority | 当时 squeue 的原因（Priority / Resources / QOSMax... / AssocGrp...）和优先级 |
| obs_slurm_est_start, obs_slurm_est_wait_h | 当时 Slurm 自己估计的开始时间 / 估计等待（小时），可以当 baseline |
| obs_qos_all_running/pending | 当时这个 QoS 全集群的运行 / 排队数（集群不让看别人的 job 时为空） |
| obs_qos_mine_running/pending | 当时我自己在这个 QoS 的运行 / 排队数 |
| obs_bl_* | 当时的积压估计（见主 README）：分区、CPU 积压范围、GPU 积压范围、瓶颈、积压数据有多旧（分钟） |

## qos_snapshots.csv —— 每次刷新追加

每个集群每个 QoS 一行：observed_at（本机时间）、全集群 / 我的 运行 / 排队数、限额。

## partition_snapshots.csv —— 积压数据更新时追加（默认最多 5 分钟一次）

每个分区一行（gpu_type 为空），有 GPU 的分区再每种卡一行：总核数、总卡数、积压范围、我的运行 / 排队数。
backlog_at 是 UTC 时间。

## 注意

- 只在刷新时采样。默认不自动刷新，所以想多攒数据可以设 `auto_refresh_minutes`（比如 15）。
- obs_* 是"第一次看到它排队时"的状态，不一定正好是提交那一刻；要更准可以按时间把快照表对上。
- 只记录你自己的 job（squeue --me / sacct）。
