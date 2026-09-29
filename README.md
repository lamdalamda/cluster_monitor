# HPC Job Dashboard

在你自己的工作电脑上跑一个小网页，实时看多个 Slurm 集群上你的作业状态、
跑在哪个节点、以及作业之间的 dependency 链条（按依赖关系自动展开成一张图）。

## 原理

后台线程按固定间隔（config.yaml 里 `poll_interval_seconds`）对每个集群跑：
- `squeue --me -o ...` 拿当前排队/运行中的作业
- `sacct -X --starttime=now-Nhours ...` 拿最近 N 小时内已结束的作业（这样依赖链
  里指向"已经跑完/失败"的job时，也能显示出它的最终状态，而不是一个空白节点）

然后把两部分数据按 job id 合并，用 squeue 里的 `%E`（dependency 字段）画出
job 之间的依赖边，前端用 vis-network 渲染成一张从左到右的有向图。

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

- 每个节点 = 一个 job，颜色代表状态（RUNNING 绿 / PENDING 黄 / COMPLETED 灰 /
  FAILED 红 / CANCELLED 橙 ...），跑起来的作业节点上会直接显示在哪台机器上跑。
- 边（箭头）= dependency，从"被依赖的job"指向"依赖它的job"。灰色虚线代表依赖
  已经满足（前置job已COMPLETED），黄色实线代表还没满足（还在跑/排队/失败）。
- 点节点看详情面板（原始 dependency 字符串、reason、时间等）。
- 顶部可以按集群筛选、按 job id / 名字 / 节点名搜索。
- "立即刷新"按钮会强制马上去 SSH 拉一次最新数据，不用等下一个轮询周期。

## 常见调整

- **只想看某几个集群**：在 `config.yaml` 里把不需要的集群 `enabled: false` 或删掉。
- **轮询太慢/太快**：改 `poll_interval_seconds`（SSH 有开销，别设太小，10-20秒合适）。
- **依赖链历史看不全**：加大 `history_hours`（sacct 查询范围），但太大会让每次
  轮询变慢。
- **想在手机/同事电脑上也能看**：把 `config.yaml` 里 `host` 改成 `0.0.0.0`，
  然后用你工作电脑的局域网 IP 访问。注意这样局域网内的人都能看到你的作业名字
  等信息，公司网络环境要自己权衡安全性。
- **SGE**：这个工具目前只写了 Slurm 的 squeue/sacct 解析。你提到 SGE 快退役
  了就没做；如果还想要，思路一样，把 `parse_squeue`/`parse_sacct` 换成解析
  `qstat -xml` 就行，需要的话可以再让我加。

## 已知限制

- Dependency 目前只处理同一个集群内部的作业 id（squeue 的依赖本来也基本是
  集群内的，Slurm 本身不支持跨集群 dependency）。
- Array job（`123_4` 这种）依赖解析是按整数 id 匹配的，正常情况没问题，
  但非常复杂的 array 依赖表达式（比如依赖某个 array 里的部分 index）不会
  精细区分到具体 index。
- 没做鉴权，默认只监听 127.0.0.1，公司电脑上一般够用；如果开 0.0.0.0 建议
  自己加一层反向代理 + 基本认证。
