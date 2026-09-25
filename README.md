# ds-pool：DeepSeek 网页版号池网关

把 `chat.deepseek.com` 网页版（经 [Universal-Web-API](https://github.com/llmapi-io/universal-web-api) 驱动真实 Chrome）包装成 OpenAI 兼容接口，
多台登录实例组成号池，自己做会话粘滞、增量发送、故障切换、用量记账和登录健康管理。

> 本项目是自用系统，代码质量以「能稳定跑」为先，不是标准开源库。接口与配置可能随时变化，欢迎 fork 自用。

## 功能特性

- **OpenAI 兼容接口**：`POST /v1/chat/completions`、`POST /v1/responses`，支持流式输出、工具调用、图片/文件附件透传。
- **号池调度**：多实例轮询分发，每实例并发限制，超时 / 5xx / 敏感词命中自动退让并冷却换投。
- **会话粘滞 + 增量发送**：同一串历史算一个指纹，绑定到某台实例的网页会话上；后续请求只发新增回合，省 60–80% 上行文本。
- **SSE 心跳**：流式长连接定期发送 keepalive，避免网关/客户端中间链路掐断。
- **用量统计**：按天记账，保留最近 8 天，接口与面板可查。
- **管理面板**：`/ui` Web 面板（token 鉴权），`/health`、`/pool/stats`、`/pool/sessions` 可视化状态。
- **登录健康探针**：通过 CDP 检查每台 Chrome 的登录态，自动重登、封号停机、到点自动开机，全程不打断在途请求。

## 架构

```
                 ┌──────────────────────────────────────────────┐
客户端 ──HTTP──▶ │  ds-pool 网关 (pool.py)                      │
                 │  轮询 / 并发限制 / 会话粘滞 / 用量记账 / 面板  │
                 └───────┬──────────────────┬───────────────────┘
                         │                  │ ctl (Unix socket / TCP)
                 ┌───────▼───────┐   ┌──────▼──────────┐
                 │  UWA 实例 ×N  │   │ ctl.py 服务控制  │
                 │ (真实 Chrome) │   └──────┬──────────┘
                 └───────┬───────┘          │ systemctl
                         │ CDP              │
                 ┌───────▼────────┐         │
                 │ probe_login.py │◀────────┘
                 │ (登录态探针)    │──▶ 写 /var/lib/ds-pool/login_state.json，
                 └────────────────┘     供网关 /health 与调度层读取
```

典型部署：3 台 UWA 实例（`uwa-webapi{1,2,3}`）+ 3 台 Chrome（`chrome-webapi{1,2,3}`），Xvfb 虚拟桌面，
外加 4 个 systemd 服务：`ds-pool`（网关）、`ds-pool-ctl`（服务控制）、`ds-pool-probe`（登录探针），
以及每台 UWA/Chrome 一个单元。

## 目录结构

| 路径 | 作用 |
| --- | --- |
| `pool/pool.py` | 网关主体：协议转换、号池调度、面板、ctl 通道 |
| `pool/dsess.py` | 会话指纹表：会话粘滞 + 增量发送（只发新增回合） |
| `pool/ctl.py` | 服务控制守护（Unix socket / TCP），启停实例用的受限指令通道 |
| `pool/probe_login.py` | 登录健康探针（CDP 判定登录态 / 自动重登 / 封号停机） |
| `pool/ui.html` | 管理面板前端 |
| `pool/*.service` | systemd 单元（ds-pool / ctl / probe / uwa / chrome） |
| `pool/pool.env.example` | 配置样例，所有令牌都是占位符 |
| `pool/env_setup.sh` | 服务器侧就地生成 `pool.env`（从 UWA .env 读令牌，不回显） |
| `pool/switch_account.sh` `pool/profiles.conf` | 账号切换示例：账号名 ↔ Chrome user-data-dir 注册表 |
| `tests/` | 本地单测：会话语义、附件透传、payload 规整、语言指令注入 |
| `patches/` | 对上游 UWA 的补丁（附件定位符归一化等） |

## 核心机制

### 会话粘滞与增量发送（`dsess.py`）

同一串历史算一个指纹 `sid`，绑到某台实例的网页会话上；后续请求只发新增回合。
任何异常（网页会话丢了、历史被改写、换了模型、请求失败）一律退回全量重发，宁可多发不能发错。
`DS_SESS_INCREMENT=0` 可一键关闭增量发送。

### 附件透传（`split_content`）

把 chat / Responses / Anthropic 三种写法收敛成 UWA 认的 `image_url` / `file` part；
没有附件时 `content` 保持字符串，旧行为零改动。只认 `http(s)://` 和 `data:`，
本地路径与 `file_id` 直接丢弃。一张图在会话长度估算里按 `DS_SESS_IMAGE_CHARS`（默认 2000）字计权，
否则带图回合永远达不到复用阈值。

### 调度与故障切换

单实例并发默认 1（`PER_UPSTREAM_CONCURRENCY`），超时 / 5xx / 敏感词命中即退让换实例；
`COOLDOWN` 控制失败后冷却。登录状态被探针标记 `unhealthy` 的实例直接从调度中剔除，
健康池为空且无在途请求时，网关立即返回 503（`all_upstreams_unhealthy` + `Retry-After`），不干等排队超时。

### 登录健康探针（`probe_login.py`）

通过 CDP 读取每台实例的页面 URL + DOM 快照，判定登录态：

- `ok`：页面在 `chat.deepseek.com/*` 且有输入框；
- `sign_in`（掉登录）：**上轮还正常、本轮掉到登录页 = 疑似被强制下线** → 不立即重登，
  排 12–24 小时随机冷却（避免「一重登立刻被封」），冷却到期才试登一次，失败再排一轮；
  其余情况（重启后首次 / 从未正常）走 `PROBE_LOGIN_MIN`（默认 300s）短冷却自动重登；
- `banned`：页面出现「已被禁言/违反使用规范」→ 立即停掉该实例 Chrome + UWA，摘出轮询，
  按页面解封时间排 `wake_at`（解封 + `PROBE_WAKE_PAD` 缓冲）自动开机复查；
  解封时间解析不到则随机 6–12 小时复查；
- `no_input` / 无标签页：自动重启该实例 Chrome 并复查。

每次动作前先查 `ds-pool /pool/status`，实例有在途请求时延迟处理，绝不打断正在跑的流。
冷却 / 重登 / 重启时间戳持久化在 `/var/lib/ds-pool/login_cooldown.json`，探针重启不丢；
`--now <实例号>` 单次巡检同样遵守冷却，不会绕过。凭据只存服务器本地，脚本全程不回显邮箱全文 / 密码 / token。

## 部署（自建）

前置条件：Linux 服务器、Python 3、[Universal-Web-API](https://github.com/llmapi-io/universal-web-api)
实例 + 已登录网页版 DeepSeek 的 Chrome（建议 Xvfb 虚拟桌面）、systemd。

```bash
# 1) 代码放到 /opt/ds-pool，单元放到 /etc/systemd/system
cp pool/pool.py pool/dsess.py pool/ctl.py pool/probe_login.py pool/ui.html /opt/ds-pool/
cp pool/*.service /etc/systemd/system/

# 2) 生成配置文件（或手工复制 pool.env.example 为 pool.env 并填令牌）
bash /opt/ds-pool/env_setup.sh        # 就地生成 /opt/ds-pool/pool.env，权限 600

# 3) 按你的实例拓扑修改 pool.env 里的 UPSTREAM_1..N
vi /opt/ds-pool/pool.env

# 4) 启动
systemctl daemon-reload
systemctl enable --now ds-pool ds-pool-ctl ds-pool-probe
```

`UPSTREAM_n` 字段格式：`实例ID|UWA地址|模型名|显示名|UWA令牌`，例如：

```
UPSTREAM_1=ds1|http://127.0.0.1:8199|chat.deepseek.com|节点1|<UWA令牌>
```

## 配置项

全部通过环境变量注入（systemd `EnvironmentFile`），以下是主要配置：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `POOL_HOST` / `POOL_PORT` | `0.0.0.0` / `8288` | 网关监听地址 / 端口 |
| `POOL_TOKENS` | - | 管理接口令牌（逗号分隔可配多个） |
| `UPSTREAM_1..N` | - | 上游实例表，格式见上文 |
| `UPSTREAM_MODELS` | - | 上游接受的模型名白名单 |
| `MODEL_ALIAS` | - | 请求模型名 → 上游模型名映射 |
| `DEFAULT_MODEL` | - | 未指定模型时的默认值 |
| `UPSTREAM_SERVICES` | - | 实例ID → systemd 单元名映射（探针停机/开机用） |
| `UPSTREAM_TOKEN` | - | 调用 UWA 的默认令牌 |
| `PER_UPSTREAM_CONCURRENCY` | `1` | 每实例最大并发 |
| `COOLDOWN` | `300` | 实例失败后冷却秒数 |
| `QUEUE_TIMEOUT` | `180` | 全忙时排队等待秒数 |
| `FIRST_BYTE_TIMEOUT` | `90` | 上游首字节超时 |
| `KEEPALIVE` | `12` | 流式 SSE 心跳间隔秒，0 关闭 |
| `STATS_FILE` | `/var/lib/ds-pool/stats.json` | 用量统计文件 |
| `STATS_FLUSH_SECS` / `USAGE_KEEP_DAYS` | `15` / `8` | 落盘间隔 / 历史保留天数 |
| `DS_POOL_LOGIN_STATE` | `/var/lib/ds-pool/login_state.json` | 探针写的登录态文件 |
| `DS_POOL_CTL_TOKEN` / `POOL_CTL_TCP_PORT` | - / `8399` | ctl 通道令牌 / TCP 端口 |
| `POOL_LANG_DIRECTIVE` | 空 | 非空时作为第一条 system 消息注入每次请求 |
| `DS_SESS_INCREMENT` | `1` | 会话粘滞增量发送开关 |
| `DS_SESS_TTL` / `DS_SESS_MAX` | `1800` / `128` | 指纹存活秒数 / 指纹表容量 |
| `DS_SESS_IMAGE_CHARS` | `2000` | 一张图在长度估算里的名义字数 |
| `PROBE_INTERVAL` | `120` | 探针巡检间隔秒 |
| `PROBE_LOGIN_MIN` | `300` | 自动重登最小间隔 |
| `PROBE_RESTART_MIN` | `90` | 重启 Chrome 最小间隔 |
| `PROBE_WAKE_PAD` | `900` | 解封后延迟开机的缓冲秒 |
| `PROBE_LIVENESS` | `1` | 端到端探活开关（隔 N 轮发一个极短真实请求） |

## HTTP 接口

| 接口 | 鉴权 | 说明 |
| --- | --- | --- |
| `POST /v1/chat/completions` | 上游令牌 | OpenAI 兼容 Chat 补全（含流式） |
| `POST /v1/responses` | 上游令牌 | OpenAI Responses 格式 |
| `GET /health` | 无 | 无认证健康检查（不泄露敏感信息） |
| `GET /pool/stats` `GET /pool/sessions` | `POOL_TOKENS` | 用量统计 / 会话指纹表 |
| `GET /ui?token=<POOL_TOKENS>` | URL token | 管理面板 |
| `POST /pool/sessions/clear` | `POOL_TOKENS` | 清空会话指纹表（手动切号后必做） |

`/health` 示例：

```json
{
  "service": "ds-pool",
  "ok": false,
  "upstreams_ready": ["ds1", "ds2", "ds3"],
  "upstreams_healthy": [],
  "upstreams": [
    {"id": "ds1", "login_state": "parked", "login_unhealthy": true, "wake_at": 1790451660}
  ],
  "login_state_updated_at": "2026-09-25T10:00:00+08:00"
}
```

- `ok`：存在「未冷却且未被登录态标记 unhealthy」的实例；
- `upstreams_ready`：可路由（未冷却优先，语义兼容早期自检脚本）；
- `upstreams_healthy`：排除登录态 `unhealthy` 后的可用实例；
- `upstreams[]`：每实例明细（`login_state` 取探针结果，`wake_at` 为封号解封自动开机时间戳）。

## 本地验证

```powershell
python -X utf8 tests/test_dsess.py          # 会话语义：复用/换实例/改历史/失败作废
python -X utf8 tests/test_wire_local.py     # 附件透传：覆盖三种协议写法与丢弃规则
python -X utf8 tests/test_payload_wire.py   # 请求 payload 规整
python -X utf8 tests/test_lang_inject.py    # 语言指令注入
```

单测直接读 `pool/` 里的权威副本，不需要连服务器。

## 安全约定

- 仓库不入库任何凭据：`_creds/`、`*.env`、`pool.env` 都在 `.gitignore`，配置文件只用占位符；
- 探针的账号凭据只存服务器本地（`config.json`，权限 600），脚本只在登录表单里读取，
  全程不回显邮箱全文 / 密码 / token；
- `/health` 无鉴权但有意识不含敏感信息，管理接口全部要求 `POOL_TOKENS`；
- 改动实例账号配置前先备份服务器的凭据文件。

## License

MIT License，Copyright (c) 2026 binlin0120，详见 [LICENSE](LICENSE)。