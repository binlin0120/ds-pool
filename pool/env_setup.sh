#!/usr/bin/env bash
# 在服务器侧就地生成 /opt/ds-pool/pool.env：从两个 UWA 实例的 .env 里读 AUTH_TOKEN，
# 从不在命令行明文传递密钥，也不打印密钥内容（只打印长度）。
set -euo pipefail
umask 077
mkdir -p /opt/ds-pool

ENVF=/opt/ds-pool/pool.env

read_token() {
  local f="$1"
  [ -f "$f" ] || { printf ''; return 0; }
  sed -n 's/^AUTH_TOKEN=//p' "$f" | head -1 | tr -d '"' | tr -d "'" | tr -d '\r'
}

T1=$(read_token /opt/uwa/universal-web-api-main/.env)
T2=$(read_token /opt/uwa2/universal-web-api/.env)
T3=$(read_token /opt/uwa3/universal-web-api/.env)

PT=""
if [ -f "$ENVF" ]; then
  PT=$(sed -n 's/^POOL_TOKENS=//p' "$ENVF" | head -1 | tr -d '\r')
fi
if [ -z "$PT" ]; then
  PT=$(openssl rand -hex 24)
  ROTATED="新生成"
else
  ROTATED="沿用旧值"
fi

CT=""
if [ -f "$ENVF" ]; then
  CT=$(sed -n 's/^DS_POOL_CTL_TOKEN=//p' "$ENVF" | head -1 | tr -d '\r')
fi
if [ -z "$CT" ]; then
  CT=$(openssl rand -hex 24)
  CT_ROT="新生成"
else
  CT_ROT="沿用旧值"
fi

cat > "$ENVF" <<EOF
# ds-pool 配置（systemd EnvironmentFile）—— 权限 600，含密钥，勿外传
POOL_HOST=0.0.0.0
POOL_PORT=8288
POOL_TOKENS=$PT
DS_POOL_CTL_TOKEN=$CT
POOL_CTL_TCP_PORT=8399
UPSTREAM_TOKEN=$T1
UPSTREAM_1=ds1|http://127.0.0.1:8199|chat.deepseek.com|cohen-p1|$T1
UPSTREAM_2=ds2|http://127.0.0.1:8200|chat.deepseek.com|bel-p2|$T2
UPSTREAM_3=ds3|http://127.0.0.1:8201|chat.deepseek.com|upm-p3|$T3
UPSTREAM_MODELS=chat.deepseek.com,deepseek,www.chat.deepseek.com
MODEL_ALIAS=deepseek-chat=chat.deepseek.com,deepseek-reasoner=chat.deepseek.com,deepseek-v4-pro=chat.deepseek.com,deepseek-v4.1-flash=chat.deepseek.com,deepseek-v4-flash=chat.deepseek.com
DEFAULT_MODEL=chat.deepseek.com
UPSTREAM_SERVICES=ds1=uwa-webapi,chrome-webapi,ds2=uwa-webapi2,chrome-webapi2,ds3=uwa-webapi3,chrome-webapi3
PER_UPSTREAM_CONCURRENCY=1
COOLDOWN=300
QUEUE_TIMEOUT=180
FIRST_BYTE_TIMEOUT=90
KEEPALIVE=12
STATS_FILE=/var/lib/ds-pool/stats.json
STATS_FLUSH_SECS=15
USAGE_KEEP_DAYS=8
CTL_SERVICES=uwa-webapi,chrome-webapi,uwa-webapi2,chrome-webapi2,uwa-webapi3,chrome-webapi3,ds-pool
CTL_MIN_INTERVAL=3
EOF

chmod 600 "$ENVF"
if id dspool >/dev/null 2>&1; then
  chown root:dspool "$ENVF" || true
  chmod 640 "$ENVF" || chmod 600 "$ENVF"
fi

echo "pool.env 就绪 ($ROTATED)"
echo "  upstream1 token 长度=${#T1}"
echo "  upstream2 token 长度=${#T2}"
echo "  upstream3 token 长度=${#T3}"
echo "  pool token  长度=${#PT}"
echo "  ctl secret 长度=${#CT} ($CT_ROT)"
echo "  端口=8288 监听=0.0.0.0 (ufw 默认拒绝入站，公网目前打不开)"
