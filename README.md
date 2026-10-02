# HPC Job Dashboard

在你自己的工作电脑上跑一个小网页，用表格看多个 Slurm 集群上你的作业状态、
跑在哪个节点、每个 QoS 上你跑了/排了多少，方便决定下一个 job 往哪交。

## 原理

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

## 安装 & 运行

```bash
cd hpc-dashboard
pip install -r requirements.txt

# 先测试一下你能不能免密连上（本程序不处理交互式密码/2FA）
ssh clusterA squeue --me

# 编辑 config.yaml，把 clusters 换成你自己的集群和 ssh 别名
vim config.yaml

python app.py
```

然后浏览器打开 `http://127.0.0.1:8787`（端口见 config.yaml）。

## 装成 systemd 服务（开机自启）

```bash
./install.sh                                  # 会自己 sudo；默认用 pymatgen250508 那个 python
PYTHON=/path/to/python ./install.sh           # 换一个 python
./install.sh --uninstall                      # 删除服务
```

服务名 `hpc-dashboard`，以你自己的用户身份运行（用你的 `~/.ssh/config` 和 key；系统服务
拿不到桌面的 ssh-agent，所以 key 不能设密码）。看日志 `journalctl -u hpc-dashboard -f`，
改了代码或 config.yaml 之后 `sudo systemctl restart hpc-dashboard`。装之前先把手动跑的
`python app.py` 停掉，不然端口会冲突。

## 界面说明

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
  - 点 job id 跳过去，点路径复制

## 常见调整

- **只想看某几个集群**：在 `config.yaml` 里把不需要的集群 `enabled: false` 或删掉。
- **想自动刷新**：`auto_refresh_minutes: 5`（SSH 有开销，别设太小）。
- **想多看几个文件**：在 `inspect_files` 里加模式，比如 `OSZICAR`、`slurm-{id}.out`。
- **依赖链历史看不全**：加大 `history_hours`（sacct 查询范围），但太大会让每次
  刷新变慢。
- **想在手机/同事电脑上也能看**：把 `config.yaml` 里 `host` 改成 `0.0.0.0`，
  然后用你工作电脑的局域网 IP 访问。注意这样局域网内的人都能看到你的作业名字
  等信息，公司网络环境要自己权衡安全性。
- **SGE**：这个工具目前只写了 Slurm 的 squeue/sacct 解析，SGE 的 `qstat` 不支持。

## 已知限制

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
