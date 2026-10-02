# HPC Job Dashboard

在你自己的工作电脑上跑一个小网页，用表格看多个 Slurm 集群上你的作业状态、
跑在哪个节点、每个 QoS 上你跑了/排了多少，方便决定下一个 job 往哪交。

## 原理

后台线程按固定间隔（config.yaml 里 `poll_interval_seconds`）对每个集群**一次 SSH**
跑这几条命令（各集群并行）：
- `squeue --me -o ...` 拿当前排队/运行中的作业（含 QoS、依赖、预计开始时间）
- `sacct -X --starttime=now-Nhours ...` 拿最近 N 小时内已结束的作业
- `squeue -t RUNNING,PENDING -o '%q|%T'`（所有用户）统计每个 QoS 全集群的运行/排队数
- `sacctmgr show assoc / show qos`：你能用哪些 QoS、每个 QoS 的限额
  （这个基本不变，默认 30 分钟才查一次）

**这不是通过 API/REST 连集群，就是普通的 `ssh <host> <command>`**，所以：
- 需要你本机能免密 SSH 到各集群登录节点（SSH key + agent，或者 key 没设密码）
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

**Job 表格**
- 同一个集群里**名字相同**的 job（一般就是一条 dependency 链，或者同一批重复提交）
  收成一行，`×N` 是个数，点 ▶ 展开看每一个。折叠行显示的是"当前"那个 job：
  正在跑的 > 下一个排队的 > 最后一个；旁边的小标签是这组里各状态的数量（R 运行、
  PD 排队、CD 完成、F 失败、CA 取消、TO 超时…）
- 排序：有 job 在跑的组在最上面，然后是排队的，然后是已经结束的
- "依赖"列显示 `afterok #12345` 这种，前面的小圆点是被依赖 job 的状态颜色，
  点它会跳到那个 job
- 排队中的 job，"时间"列显示 Slurm 估计的开始时间（`预计`）
- 点 Job ID 复制（方便 `scancel` 或者 `--dependency=afterok:ID`）
- 鼠标停在名字上能看到完整信息（原始 dependency 字符串、提交/开始/结束时间等）
- 顶部可以按集群 / QoS / 状态筛选、搜索；展开状态和筛选条件会记在浏览器里
- "立即刷新"按钮会强制马上去 SSH 拉一次最新数据，不用等下一个轮询周期

## 常见调整

- **只想看某几个集群**：在 `config.yaml` 里把不需要的集群 `enabled: false` 或删掉。
- **轮询太慢/太快**：改 `poll_interval_seconds`（SSH 有开销，别设太小，10-20秒合适）。
- **依赖链历史看不全**：加大 `history_hours`（sacct 查询范围），但太大会让每次
  轮询变慢。
- **集群要 2FA / 交互式登录（比如 `Permission denied (keyboard-interactive)`）**：
  本程序用的是 `ssh -o BatchMode=yes`，不能输密码。可以在 `~/.ssh/config` 里给这个
  Host 开连接复用，先在终端手动 `ssh` 登录一次，之后 dashboard 会复用这条连接：
  ```
  Host sulis
      ControlMaster auto
      ControlPath ~/.ssh/cm-%r@%h:%p
      ControlPersist 12h
  ```
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
