#!/bin/sh
set -eu
cd "$(dirname "$0")"
target=/opt/mqtt_monitor
account=mqtt-monitor
service=mqtt_monitor.service
if [ "$(id -u)" -ne 0 ]; then
  echo 'Run this installer as root.' >&2
  exit 1
fi
if [ ! -f "$target/config.json" ] && [ ! -f config.json ]; then
  echo 'Create config.json before installing.' >&2
  exit 1
fi
if ! id "$account" >/dev/null 2>&1; then
  useradd --system --user-group --no-create-home --shell /usr/sbin/nologin "$account"
fi
install -d -m 0755 "$target"
python3 -m venv "$target/.venv"
"$target/.venv/bin/python" -m pip install -r requirements.txt
install -m 0644 mqtt_monitor.py index.html requirements.txt "$target/"
if [ ! -f "$target/config.json" ]; then
  install -o root -g "$account" -m 0640 config.json "$target/config.json"
fi

runuser -u "$account" -- "$target/.venv/bin/python" "$target/mqtt_monitor.py" --check
install -m 0644 "$service" "/etc/systemd/system/$service"
systemctl daemon-reload
systemctl enable "$service"
systemctl restart "$service"
systemctl is-active "$service"
