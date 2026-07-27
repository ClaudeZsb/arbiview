#!/usr/bin/env bash
set -u

TUNNEL_PORT="${ARBIVIEW_TUNNEL_PORT:-18080}"
SSH_TARGET="${ARBIVIEW_SSH_TARGET:-lightsail-freqtrade}"
TUNNEL_PID=""

cleanup() {
  if [[ -n "$TUNNEL_PID" ]]; then
    kill "$TUNNEL_PID" 2>/dev/null || true
    wait "$TUNNEL_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

if ! curl --noproxy '*' --silent --fail --max-time 2 "http://127.0.0.1:${TUNNEL_PORT}/health" >/dev/null; then
  (
    while true; do
      ssh -N \
        -L "${TUNNEL_PORT}:127.0.0.1:8080" \
        -o ExitOnForwardFailure=yes \
        -o ServerAliveInterval=15 \
        -o ServerAliveCountMax=6 \
        -o TCPKeepAlive=yes \
        "$SSH_TARGET"
      echo "ArbiView SSH tunnel disconnected; retrying in 2 seconds..." >&2
      sleep 2
    done
  ) &
  TUNNEL_PID=$!

  for _ in {1..30}; do
    if curl --noproxy '*' --silent --fail --max-time 2 "http://127.0.0.1:${TUNNEL_PORT}/health" >/dev/null; then
      break
    fi
    sleep 1
  done
fi

env -u HTTP_PROXY -u HTTPS_PROXY -u http_proxy -u https_proxy next dev --port 3000
