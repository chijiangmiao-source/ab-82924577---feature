# 高空探测器标定包发布系统

操作员在控制台输入**发布标识**与 **Base64 工件（≤ 64KiB）**，控制服务将同一候选字节
切换至**两座离线镜像仓**（repo-a / repo-b），并可按标识查看进度、当前摘要及准备 /
激活证据。仓库内自带名为 **verify** 的一次性验收服务，以退出码报告验收结果。

## 架构

```
            ┌────────────┐   prepare/activate（仓端操作键幂等）  ┌────────┐
操作员 ───▶ │  control   │ ──────────────────────────────────▶ │ repo-a │
（浏览器）  │ 控制台+API │ ──────────────────────────────────▶ │ repo-b │
            └────────────┘                                     └────────┘
                  │ SQLite（WAL, synchronous=FULL）持久化：发布意图 + 仓端回执
            ┌────────────┐
            │   verify   │ 一次性验收：断连/重启场景 → 代码测试 → 构建检查 → HTTP 冒烟
            └────────────┘
```

全部组件仅使用 Python 标准库（零第三方依赖），镜像构建不需要任何网络下载。

## 快速开始

```bash
# 启动全部服务（含一次性验收）
docker compose up --build

# 仅以验收退出码作为结果（CI 用法）
docker compose up --build --exit-code-from verify verify

# 自定义宿主机端口（默认 8080）
CONTROL_PORT=9090 docker compose up --build -d control repo-a repo-b
```

控制台：`http://localhost:8080/`（健康响应：`GET /healthz` → `{"status":"ok"}`）。

## 关键语义

- **先持久化，后执行**：`POST /api/releases` 先把 `sha256` 与不可变发布意图
  （标识 → 摘要 + 字节）写入 SQLite，再由后台协调器驱动仓端操作。
- **单一激活栅栏**：全部发布按首次持久化顺序（`seq`）排队，任意时刻只有
  顺位最旧的未决发布持有栅栏并进入协调，因此两仓活动指针永远不会分别
  归属互不相干的发布。提交新发布时，淘汰决策与新意图在**同一事务**落盘：
  - 更旧的未决候选若**未留下任何仓端激活证据**（无 activate 回执），
    立即被新发布淘汰为 `SUPERSEDED`（终态），详情与列表中的
    `superseded_by` 指明取代者，且它永不再触发准备或激活；
  - 更旧的候选若**已留下任一仓的持久化激活证据**，新发布进入
    `WAITING`（`blocked_by` 指明阻塞者），待旧候选依据既有回执收敛为
    `COMPLETED` 或 `REJECTED` 后，再依序进入协调。
  栅栏切换、仓端响应丢失或控制服务重启后，队列结论都从持久化意图与
  仓端回执恢复，不会漂移。
- **仓端操作键**：`rel:{发布标识}:{仓}:{prepare|activate}`，由发布标识派生。
  仓端按键幂等：同键同摘要回放**首次回执**（receipt_id 不变、激活计数不增）；
  同键异摘要返回 `409 op_key_conflict` 明确拒绝。
- **完成条件**：仅当两仓的激活回执摘要都等于发布 SHA-256 时才置 `COMPLETED`，
  此时 `current_digest` 才对外可见。
- **断连收敛**：若一仓在持久化激活后断开响应，控制服务重启后会先向仓端
  `GET /v1/ops/{op_key}` 认领既有回执，依据仓端回执收敛为完成，绝不二次激活。
- **拒绝锁定**：任一仓返回不属于该发布的摘要（或证据签名不符、操作键冲突）时，
  发布锁定为 `REJECTED`，`current_digest` 保持为空，仓端活动指针不被改写，
  且状态不再漂移。
- **幂等提交**：相同标识 + 相同工件 → `200` 回放当前状态，不产生第二次激活；
  相同标识 + 不同工件 → `409 release_id_in_use`，既有成功发布的真实状态保留；
  非法 Base64 → `400 invalid_base64`；超限工件 → `413 artifact_too_large`
  （恰为 64KiB 可正常发布）。

## API 摘要（控制服务）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 控制台页面（发布表单 + 反馈区 + 按标识查询证据 + 栅栏队列） |
| GET | `/healthz` | 健康响应（含 `boot_id`） |
| POST | `/api/releases` | 提交 `{release_id, artifact_b64}` → `202/200/409/400/413` |
| GET | `/api/releases/{id}` | 顺位、进度、当前摘要、双仓准备/激活证据（签名回执）、`superseded_by` / `blocked_by` |
| GET | `/api/releases` | 全部发布列表（含顺位 `seq` 与 `superseded_by`） |

仓端（仅内部网络）：`POST /v1/prepare`、`POST /v1/activate`、
`GET /v1/ops/{op_key}`、`GET /v1/state`、`GET /healthz`。

## verify 验收服务

`verify` 是执行后退出的一次性服务，按顺序执行：

1. **断连/重启场景**：武装 repo-b「激活提交后断开响应」→ 提交发布 →
   确认卡在未完成态 → 重启控制服务（新 `boot_id`）→ 恢复 repo-b →
   校验双仓最终摘要、激活次数恰为 1、准备/激活证据完整且为首次回执回放。
2. **激活栅栏场景**：双仓断开时连续提交 → 旧候选被新候选淘汰（列表与详情
   均显示取代者）→ 恢复后仅新候选激活；再武装 repo-b 断连 → 提交携带
   激活证据的持有者与等待者 → 重启控制服务 → 校验队列结论恢复（持有者
   不被淘汰、等待者仍等待）→ 持有者收敛后等待者才依序完成，等待者全程
   未提前激活，双仓最终指针同属最新发布。
3. **代码测试**：`python -m unittest discover`（单元 + 进程内集成测试，
   集成测试覆盖同一断连/重启场景与栅栏的淘汰/等待/恢复语义）。
4. **构建检查**：`python -m compileall` 字节编译全部源码。
5. **HTTP 冒烟**：健康页与发布接口（重复提交、标识复用、非法 Base64、
   超限与 64KiB 边界、拒绝锁定、仓端幂等直测、未知标识 404）。

退出码 `0` = 验收通过，非 `0` = 存在失败项（日志中逐条标注 `[FAIL]`）。

## 配置

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `CONTROL_PORT` | `8080` | 宿主机暴露控制台的端口 |
| `REPO_A_SECRET` / `REPO_B_SECRET` | `dev-secret-*` | 仓端证据 HMAC 签名密钥 |
| `FAULT_HOOKS` | `1`（compose 验收环境） | 故障注入开关，生产应置 `0` |
| `WORKER_INTERVAL_S` | `0.5` | 控制服务协调器轮询间隔 |

故障注入接口（仅 `FAULT_HOOKS=1` 时挂载）：仓端 `/fault/disconnect`、
`/fault/recover`、`/fault/disconnect-after-activate`、
`/fault/corrupt-next-activate`；控制端 `/fault/restart`（进程退出，
由 `restart: on-failure` 拉回，用于验收重启收敛）。

## 本地开发（无 Docker）

```bash
python3 -m unittest discover -s tests -t .        # 全部测试
python3 -m compileall -q app tests                # 构建检查
REPO_NAME=repo-a PORT=8001 DATA_DIR=/tmp/ra FAULT_HOOKS=1 python3 -m app.repo.server &
REPO_NAME=repo-b PORT=8002 DATA_DIR=/tmp/rb FAULT_HOOKS=1 python3 -m app.repo.server &
PORT=8080 DATA_DIR=/tmp/ctl FAULT_HOOKS=1 python3 -m app.control.server
```
