#!/usr/bin/env bash
# 轻量复检：两条非流式 + 一条流式 + 池状态 + 日志（密钥只在脚本内部读取，不外泄）。
set -uo pipefail
PT=$(sed -n 's/^POOL_TOKENS=//p' /opt/ds-pool/pool.env | head -1 | tr -d '\r\n')
B=http://127.0.0.1:8288

echo "== /health =="; timeout 10 curl -sS "$B/health"; echo
for i in 1 2; do
  echo "== 非流式 #$i =="
  timeout 120 curl -sS -D /tmp/_h$i -o /tmp/_b$i -H "Authorization: Bearer $PT" -H 'Content-Type: application/json' \
    -d "{\"model\":\"deepseek-chat\",\"messages\":[{\"role\":\"user\",\"content\":\"用一句话说今天星期几最有把握的说法，第$i次\"}]}" "$B/v1/chat/completions"
  grep -i -E '^(x-pool-upstream|HTTP)' /tmp/_h$i | tr -d '\r'
  timeout 5 python3 -c "import json;d=json.load(open('/tmp/_b$i'));print('  content=',(d['choices'][0]['message']['content'] or '')[:60]);print('  usage=',d.get('usage'))" 2>&1 | head -4
done
echo "== 流式 =="
timeout 180 curl -sS -N -H "Authorization: Bearer $PT" -H 'Content-Type: application/json' \
  -d '{"model":"deepseek-chat","stream":true,"messages":[{"role":"user","content":"只说三个字：好极了"}]}' \
  "$B/v1/chat/completions" | tr -d '\r' | tail -6
echo "== 池状态 =="
timeout 10 curl -sS -H "Authorization: Bearer $PT" "$B/pool/status" | \
  python3 -c "import json,sys;d=json.load(sys.stdin);print(' stats',d['stats']);[print('  ',u['id'],u['state'],'ok=%d fail=%d lat=%.1fs'%(u['ok'],u['fail'],u['lat_ema'])) for u in d['upstreams']]"
echo "== 日志 =="
journalctl -u ds-pool -n 8 --no-pager -o cat
echo "== 内存 =="; free -m | sed -n '2p'
echo "__SMOKE_DONE__"
