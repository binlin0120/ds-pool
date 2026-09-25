#!/usr/bin/env bash
# ds-pool 切号脚本：把某台实例的 Chrome 切换到指定账号的 profile（无需重新登录）。
#
# 用法: switch_account.sh <ds1|ds2|ds3> <bel|cohen|upm>
# 流程: 校验 -> 写指针 -> 停 Chrome -> 起 Chrome(读指针) -> 等 CDP -> 同步 UWA .env
#        -> 重启 UWA(复用已就绪的 Chrome) -> 触发一次针对该实例的健康探针(必要时自动重新登录)
set -uo pipefail

N="$1"
ACCT="$2"
CONF=/opt/ds-pool/profiles.conf
CTL_DIR=/opt/ds-pool

case "$N" in
  ds1) SUF=""    ; SVC=chrome-webapi ;;
  ds2) SUF=2     ; SVC=chrome-webapi2 ;;
  ds3) SUF=3     ; SVC=chrome-webapi3 ;;
  *) echo "bad instance: $N (expect ds1|ds2|ds3)" >&2; exit 2 ;;
esac

PROFILE=$(awk -F'|' -v a="$ACCT" '$1==a {gsub(/[ \t]/,"",$2); print $2; exit}' "$CONF")
if [ -z "$PROFILE" ]; then
  echo "unknown account: $ACCT ($(cut -d'|' -f1 "$CONF" | tr '\n' ' '))" >&2
  exit 2
fi
if [ ! -d "$PROFILE" ]; then
  mkdir -p "$PROFILE"
  chown webapi:webapi "$PROFILE" 2>/dev/null || true
fi

echo "切换 $N -> $ACCT ($PROFILE)"
echo "$PROFILE" > "/home/webapi/.active_profile.ds$N"
chown webapi:webapi "/home/webapi/.active_profile.ds$N"

# UWA .env 的 BROWSER_PROFILE_DIR 同步成同一份 profile，兜底 UWA 自己拉 Chrome 的竞态
UWA_ENV="/opt/uwa${SUF}/universal-web-api/.env"
if [ -f "$UWA_ENV" ]; then
  grep -q '^BROWSER_PROFILE_DIR=' "$UWA_ENV" && \
    sed -i "s|^BROWSER_PROFILE_DIR=.*|BROWSER_PROFILE_DIR=$PROFILE|" "$UWA_ENV" || \
    echo "BROWSER_PROFILE_DIR=$PROFILE" >> "$UWA_ENV"
fi

echo "停止 $SVC"
systemctl stop "$SVC.service"
sleep 3
echo "启动 $SVC (profile=$PROFILE)"
systemctl start "$SVC.service"

CDP=$((9221 + ${N#ds}))
ok=0
for i in $(seq 1 30); do
  if curl -s -m 3 "http://127.0.0.1:$CDP/json" >/dev/null 2>&1; then ok=1; break; fi
  sleep 2
done
[ "$ok" = 1 ] && echo "CDP :$CDP 就绪 (${i}s)" || echo "WARN: CDP :$CDP 60s 内未就绪"

UWA_SVC="uwa-webapi${SUF}"
echo "重启 $UWA_SVC"
systemctl restart "$UWA_SVC.service"

# 针对本实例做一次健康检查（若停在登录页会自动尝试重新登录）
if [ -f "$CTL_DIR/probe_login.py" ] && [ -x "/opt/uwa/venv/bin/python" ]; then
  echo "触发健康探针: $N"
  /opt/uwa/venv/bin/python "$CTL_DIR/probe_login.py" --now "${N#ds}" 2>&1 | sed -E 's/([A-Za-z0-9._%+-]+)@/\1@/g' | grep -vE 'PASSWORD|pwd' || true
fi

echo "完成: $N -> $ACCT"
