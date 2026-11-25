# MQTT Monitor

Part of a home monitoring setup built around three independent Python programs. Deye Monitor reads a solar inverter, Gree Monitor reads and controls a heat pump, and MQTT Monitor collects readings published by devices such as Shelly. Each has a local web dashboard and its own Python environment.

Companion projects: [Deye Monitor](https://github.com/marcobrianza/deye_monitor) · [Gree Monitor](https://github.com/marcobrianza/gree_monitor).

## Host and dependencies

- Python 3.10+ and the packages listed in `requirements.txt`. Run on macOS or Linux for local use and testing. On Raspberry Pi with a 64-bit Debian-based OS, the included systemd unit supports unattended operation.
- An MQTT broker such as Mosquitto 2.x, normally on TCP 1883. The broker is required for MQTT Monitor, and is optional for Deye and Gree when MQTT publication is disabled.
- InfluxDB **1.x** on TCP 8086 for historical storage. The examples target its v1 HTTP API and assume the configured database already exists. Switching off InfluxDB logging leaves the live dashboard available. Grafana is optional and can query InfluxDB using InfluxQL.
- Example configurations preserve private LAN addresses from the original setup. Set your Modbus, MQTT and InfluxDB endpoints before starting on a different network. Actual credentials belong in local files ignored by Git.

## How it works

`MQTT device → Mosquitto → MQTT Monitor → InfluxDB 1.x`

Routes match MQTT topics and choose the destination measurement, optional tags, and the payload fields to keep. Field names are copied as received. `data_path` selects a JSON object via JSON Pointer. `recursive` searches nested objects for the field names in `fields`. `include_scalar_fields` keeps all scalar fields in the selected object. The Shelly electrical example takes power and energy from one RPC topic and writes each valid message as it arrives. The dashboard shows topics, measurements, latest values, and reception timestamps.

## Run locally

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
cp config.example.json config.json
chmod 600 config.json
.venv/bin/python mqtt_monitor.py --check
.venv/bin/python mqtt_monitor.py
```

Edit `config.json` before the check. Set `mqtt.username` and `mqtt.password` for an authenticated broker. Set `influx.enabled` to `true` to enable logging. If InfluxDB requires authentication, add `influx.username` and `influx.password`. Open http://127.0.0.1:8768. Bind to `0.0.0.0` in the web settings to make the dashboard available on a trusted LAN.

## Raspberry Pi

On Raspberry Pi OS or Debian, install `python3-venv` if needed. After configuring `config.json`, run:

```sh
sudo bash install-raspberry.sh
sudo systemctl status mqtt_monitor.service
```

The service runs as `mqtt-monitor` in `/opt/mqtt_monitor` with an independent virtual environment. On upgrades the installed configuration takes precedence. Stop any previous instance writing the same measurements before starting it.
