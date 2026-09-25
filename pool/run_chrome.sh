#!/usr/bin/env bash
# ds-pool Chrome 指针启动器（systemd ExecStart 用）
#
# 用法: run_chrome.sh <实例号1|2|3>
#   * 从 /home/webapi/.active_profile.ds<N> 读 profile 目录（指针机制，切号=换指针）
#   * 指针缺失/为空时回退到传统目录 chrome_profile[2|3]，保证旧部署无缝升级
#   * 目录不存在则创建并 chown，避免新号首次启动被权限挡住
#   * exec 直接替换进程，保证 systemd 能正确跟踪 Chrome 主进程
set -u

N="${1:-1}"
PIPE="/home/webapi/.active_profile.ds${N}"
PROFILE=""

if [ -f "$PIPE" ]; then
  PROFILE=$(cat "$PIPE" | tr -d '\r\n' | sed 's/[[:space:]]*$//')
fi
if [ -z "$PROFILE" ]; then
  case "$N" in
    2) PROFILE=/home/webapi/chrome_profile2 ;;
    3) PROFILE=/home/webapi/chrome_profile3 ;;
    *) PROFILE=/home/webapi/chrome_profile ;;
  esac
fi
if [ ! -d "$PROFILE" ]; then
  mkdir -p "$PROFILE" 2>/dev/null || true
  chown webapi:webapi "$PROFILE" 2>/dev/null || true
fi

CDP=$((9221 + N))

exec /opt/google/chrome/chrome \
  --user-data-dir="$PROFILE" \
  --remote-debugging-port=$CDP \
  --no-first-run --no-default-browser-check \
  --disable-blink-features=AutomationControlled \
  --lang=zh-CN \
  --window-position=0,0 --window-size=1920,1080 \
  --renderer-process-limit=2 \
  --disable-dev-shm-usage \
  --disable-background-timer-throttling \
  --disable-renderer-backgrounding \
  --disable-backgrounding-occluded-windows \
  --disable-features=Translate,AutofillServerCommunication,OptimizationHints \
  --no-service-autorun --report-upload=false \
  "https://chat.deepseek.com/"
