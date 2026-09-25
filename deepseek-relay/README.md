# deepseek-relay

一个**零第三方依赖**（只用 Python 标准库）的 DeepSeek / OpenAI 兼容反向代理。
它存在的唯一理由：把"上游五花八门的失败"和"回给客户端的错误"之间建立**可诊断的映射**，
而不是像普通反代那样把所有失败都压成一个没人看得懂的 429。

> 写这个项目的直接起因：一台阿里云服务器上 `ds2api`（网页版反代）+ `new-api` + `nginx`
> 三层链路一直返回 429，客户端"无法返回上游"。实测拆开后发现 4 个互不相干的 429 来源
> 被叠在了一起，其中**一个是 nginx 自己造的**。完整归因见
> `../docs/诊断结论-2026-09-17-实测复盘.md`。

## 三条设计红线（都是踩出来的）

1. **上游容量类 429 永远不扣 key 健康分。** 否则"上游满了 → 全池进冷却 → 网关自己
   变成一直 429"，等于把故障从上游复制到自己身上。
2. **错误永远是 JSON。** nginx 默认 `limit_req_status 429` 吐的是 HTML，OpenAI SDK
   解析不出内容类型，报出来的就是"没有返回上游"这种玄学。
3. **绝不透传入站指纹。** `User-Agent` / `X-Forwarded-For` / `CF-*` / `Sec-*` 一律丢掉，
   只白名单保留 `authorization`、`content-type`、`accept`。机房 IP + 随机客户端指纹
   是最容易被风控钉死的组合。

## 它做什么

```
客户端 ──HTTP──► L1 入口（鉴权 / 每真实IP令牌桶 / 并发闸门）
                    │
                    ▼
                L2 治理（triage 分诊 → KeyPool 选凭证 → 重试/换key/停机）
                    │
                    ▼
                L3 出口（连接超时与读超时分离 · SSE 边收边推 · HTTP CONNECT 代理）
                    │
        ┌───────────┼────────────────────┐
        ▼           ▼                    ▼
 api.deepseek.com  ds2api:8788      任意 OpenAI 兼容上游
 （官方，推荐）    （网页版，需住宅出口）
```

| 模块 | 文件 | 职责 |
|---|---|---|
| 分诊 | `relay/triage.py` | 把 status+headers+body(+SSE 帧) 判成 13 种 `Verdict`，决定 重试 / 换 key / 换出口 / 是否罚凭证 |
| 号池 | `relay/pool.py` | 平滑加权轮询 + 滑窗熔断（60s 起、×2、封顶 600s）+ 隔离 1800s + 探测回池；**容量类错误只做软退避** |
| 出口 | `relay/egress.py` | `http.client` 实现：连接超时/读超时分离、`set_tunnel` 走 HTTP 代理、头白名单 |
| 限流 | `relay/limiter.py` | 每真实 IP 令牌桶（**返回 JSON 429 + Retry-After**）、并发闸门 |
| 入口 | `relay/server.py` | 路由 + 重试环 + 首包探伤 + SSE 立即 flush + 结构化日志 + `/relay/status` |

## 5 分钟跑起来

```powershell
# Windows（PowerShell 7）
cd D:\codex工作\反代\deepseek-relay
$env:RELAY_KEYS = "sk-****"                      # 逗号分隔可放多个
$env:RELAY_CLIENT_KEYS = "ck-local-1"            # 你的客户端用它连网关
python -m relay --print-config                  # 先看解析出来的配置（密钥已打码）
python -m relay                                  # 监听 127.0.0.1:8790
```

```bash
# Linux / 服务器
export RELAY_HOST=127.0.0.1 RELAY_PORT=8790
export RELAY_KEYS="sk-***"
export RELAY_CLIENT_KEYS="ck-app-1,ck-app-2"
python3 -m relay
```

客户端只改一行：`base_url = "http://127.0.0.1:8790/v1"`，`api_key = "ck-app-1"`。

验证：

```bash
curl -s http://127.0.0.1:8790/healthz
curl -s http://127.0.0.1:8790/relay/status                     # 需本机或 RELAY_ADMIN_TOKEN
curl -s -X POST http://127.0.0.1:8790/v1/chat/completions \
  -H "Authorization: Bearer ck-app-1" -H "Content-Type: application/json" \
  -d '{"model":"deepseek-chat","messages":[{"role":"user","content":"你好"}],"stream":true}'
```

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `RELAY_KEYS` | 空 | 上游凭证，逗号分隔。留空=不鉴权直连（只适合本地调试） |
| `RELAY_KEY_RPM` / `RELAY_KEY_BASE_URL` | 空 | 与 `RELAY_KEYS` 按位对齐的每 key RPM / 独立 base_url |
| `RELAY_CLIENT_KEYS` | 空 | 入站 key（客户端用它）。**留空=入站不鉴权，必须只监听 127.0.0.1** |
| `RELAY_BASE_URL` | `https://api.deepseek.com` | 上游基址。前置 ds2api 时填 `http://127.0.0.1:8788/v1` |
| `RELAY_API_PATH` | `/chat/completions` | 拼在 base_url 后面的对话路径 |
| `RELAY_HOST` / `RELAY_PORT` | `127.0.0.1` / `8790` | 监听地址。**不要为了省事改成 0.0.0.0**，用 nginx 收口 |
| `RELAY_MAX_ATTEMPTS` | 3 | 单请求最多尝试次数（跨凭证） |
| `RELAY_CONNECT_TIMEOUT` | 10 | 连接超时（秒） |
| `RELAY_READ_TIMEOUT` | 300 | 读超时，要覆盖长 SSE |
| `RELAY_MAX_INFLIGHT` | 16 | 并发闸门，满了回 503 JSON（不是挂死） |
| `RELAY_INBOUND_RPM` / `RELAY_INBOUND_BURST` | 120 / 40 | 每真实 IP 令牌桶。回的是**带 Retry-After 的 JSON 429** |
| `RELAY_WINDOW_SECONDS` / `RELAY_TRIP_THRESHOLD` | 120 / 3 | 滑窗内几次"真·该凭证的错"才进冷却 |
| `RELAY_COOLDOWN_BASE` / `..._MAX` / `..._ISOLATION` | 60 / 600 / 1800 | 冷却指数退避与隔离时长 |
| `RELAY_PROXY_URL` | 空 | `http://127.0.0.1:8118` 形式（CONNECT 隧道，TLS 仍本地校验）。**不支持 socks5://**，需要 socks 时前面垫 privoxy |
| `RELAY_USER_AGENT` | `deepseek-relay/1.0` | 出口统一 UA，避免把客户端指纹带给上游 |
| `RELAY_ADMIN_TOKEN` | 空 | `/relay/status`、`/relay/metrics` 的 Bearer；空=仅本机 |

## 三种上游用法

```bash
# ① 官方 API（推荐，本机实测 TLS 握手 0.17s，机房 IP 未被风控）
export RELAY_BASE_URL=https://api.deepseek.com RELAY_KEYS="sk-***,sk-***"

# ② 前置 ds2api（网页版转的 OpenAI 接口）：网关负责分诊与号池外治
export RELAY_BASE_URL=http://127.0.0.1:8788/v1 RELAY_KEYS="sk-anything"

# ③ 任意 OpenAI 兼容上游（含 new-api 之类的聚合站）
export RELAY_BASE_URL=https://other-provider.example/v1 RELAY_KEYS="..."
```

`RELAY_KEYS` 在模式 ②/③ 下是"上游站点发给你的 key"，不是 DeepSeek 官方 key；
它的健康分同样受池子管理，所以**上游返回 `RISK_DEVICE_DETECTED` 会被自动下线**，
不会像现在这样把风控错误一路传染给你的客户端。

## 部署到服务器（systemd + nginx）

```bash
sudo useradd -r -s /usr/sbin/nologin relay 2>/dev/null || true
sudo mkdir -p /opt/deepseek-relay && sudo cp -r relay /opt/deepseek-relay/
sudo cp deploy/relay.env /etc/deepseek-relay.env && sudo chmod 600 /etc/deepseek-relay.env
sudo sed -i 's#^RELAY_KEYS=.*#RELAY_KEYS=sk-你的真key#' /etc/deepseek-relay.env
sudo cp deploy/relay.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now deepseek-relay
sudo cp deploy/nginx-relay.conf /etc/nginx/conf.d/relay.conf && sudo nginx -t && sudo systemctl reload nginx
```

关键文件：

| 文件 | 说明 |
|---|---|
| `deploy/relay.service` | systemd 单元。**`StartLimitIntervalSec` 必须在 `[Unit]` 段**，放 `[Service]` 会被新 systemd 忽略（现网 ds2api 的 unit 就放错了） |
| `deploy/relay.env` | 环境变量模板，落地后 `chmod 600` |
| `deploy/nginx-relay.conf` | 反代样例：限流放宽到 60r/s 且 **429 改回 JSON**（`error_page 429 = @relay429`）、SSE 三件套、`/relay/*` 只允许内网 |

 nginx 这一层必须记住三件事（都是这次实测出来的坑）：

1. **不要**把限流状态码改成 429 还让它吐 HTML。要么不限流（交给网关的 JSON 429），
   要么 `limit_req_status 429; error_page 429 = @json429;` 用 `return 429 '{...}'` 回 JSON。
2. **SSE 必须** `proxy_buffering off; proxy_cache off; add_header X-Accel-Buffering no;`
   并把 `proxy_read_timeout` 放到 300s+，否则长回答会被掐或整段憋住。
3. 网关自己已经做了每真实 IP 令牌桶，nginx 层的 `rate` 只做粗兜底（≥60r/s 或按 IP 20r/s），
   别再用 `2r/s` 这种会掐死正常用户的值。

## 排障速查：看到 429/401/503 先做这三步

第 0 步永远先用探针分层（它会直接告诉你"这个 429 是谁造的"）：

```bash
python tools/probe.py http://127.0.0.1:8787 sk-anything --n 12   # 打现网那条链路
python tools/probe.py http://127.0.0.1:8790/v1 ck-smoke --n 3 --stream
```

```bash
curl -s localhost:8790/relay/status | python3 -m json.tool   # 池子是否全冷却
journalctl -u deepseek-relay -n 50 --no-pager                # 每请求一行，含 rid/kind/耗时
grep -c 'limiting requests' /var/log/nginx/error.log          # 是不是 nginx 自伤
```

| 回给客户端 | `verdict` | 含义 | 该动谁 |
|---|---|---|---|
| 429 JSON + `Retry-After` | `rpm` / `tpm` | 上游按 key 限流 | 等；或加 key 分摊 |
| 429 JSON | `upstream_capacity` | 上游整体满了，**与你的 key/IP 无关** | 退避重试；别换 key 也别罚 key |
| 429 JSON | `unknown_burst` | 裸 429 无 code，短时突发 | 换 key 重试（已自动做） |
| 503 JSON | `waf_block` | 边缘 WAF 拦页面（`Request Blocked`/`Block-Event-Id`） | **换出口**，重试同一出口只会加重 |
| 503 JSON | `credential_risk` | 网页版风控（`RISK_DEVICE_DETECTED`） | 换住宅出口 + 重做设备指纹，账号已自动下线 |
| 401 JSON | `auth_invalid` | 官方 governor / key 失效 | 换 key，不用换 IP |
| 402 JSON | `quota` | 余额用尽/欠费 | 充值或换 key（该 key 已下线） |
| 400 JSON | `client_error` | 模型名/参数本身错 | 改客户端，**绝不重试** |
| 502 JSON | `upstream_error` / `network_error` | 上游 5xx 或连不上 | 查上游进程与安全组 |
| 429 **HTML** | — | 100% 是 nginx `limit_req` 造的，不是上游 | 改 nginx，见 `deploy/nginx-relay.conf` |

## 明确不做的事（避免误用）

- 不做 ChatGPT/DeepSeek **网页版登录与 Cookie/设备指纹维护**（那是 `ds2api`/`ChatGPT-Web2API` 的活）。
  要网页版就 `RELAY_BASE_URL` 指向它们，本层只管分诊、号池、限流、错误语义。
- 不做 socks5 出口（标准库没有 socks 客户端）。`RELAY_PROXY_URL` 只支持 `http://host:port`
  的 CONNECT；需要 socks/住宅代理时前面垫一层 privoxy。
- 不做 Web 管理台。观测面只有 `/healthz`、`/relay/status`、`/relay/metrics`（Prometheus 文本）。
- 不承诺绕过风控。DeepSeek 网页版的对抗是长期的，见诊断报告"路线 ②"。

## 测试

```powershell
cd D:\codex工作\反代\deepseek-relay
python -m unittest tests.test_triage tests.test_relay -v     # 20 例，含真 socket 报文校验
```

`tests/golden/*.json` 是从现网抓回来的真实响应（nginx HTML 429、`RISK_DEVICE_DETECTED`、
官方 `governor` 401、WAF `Block-Event-Id`、capacity/quota/rpm/tpm 等），
每条都被断言锁定了分类结果，防止以后改分诊规则时把某类 429 悄悄降级成"罚 key"。