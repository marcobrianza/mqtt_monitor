#!/usr/bin/env python3
"""Route MQTT messages to InfluxDB 1.x and display the live feed."""
import argparse
from base64 import b64encode
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
from pathlib import Path
from queue import Full, Queue
import re
import socket
from subprocess import SubprocessError, check_output
import sys
from string import Formatter
from threading import Lock, Thread
from time import time_ns
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import paho.mqtt.client as mqtt

BASE = Path(__file__).resolve().parent
MISSING = object()


def dashboard_urls(host, port, broker):
    if host != '0.0.0.0':
        return [f'http://{host}:{port}']
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            probe.connect((broker['host'], broker['port']))
            address = probe.getsockname()[0]
            if not address.startswith('127.'):
                return [f'http://{address}:{port}']
    except OSError:
        pass
    try:
        if sys.platform == 'darwin':
            output = check_output(['/sbin/ifconfig'], text=True, timeout=2)
            addresses = re.findall(r'^\s+inet (\d+\.\d+\.\d+\.\d+) ', output, re.M)
        else:
            addresses = check_output(['hostname', '-I'], text=True, timeout=2).split()
        urls = [f'http://{ip}:{port}' for ip in dict.fromkeys(addresses)
                if '.' in ip and not ip.startswith(('127.', '169.254.'))]
        if urls:
            return urls
    except (OSError, SubprocessError, ValueError):
        pass
    return [f'http://127.0.0.1:{port}']


def pointer(value, path):
    for token in path.split('/')[1:]:
        token = token.replace('~1', '/').replace('~0', '~')
        if isinstance(value, list) and token.isdecimal() and int(token) < len(value):
            value = value[int(token)]
        elif isinstance(value, dict) and token in value:
            value = value[token]
        else:
            return MISSING
    return value


def escape(value, chars):
    value = str(value).replace('\\', '\\\\')
    return ''.join('\\' + char if char in chars else char for char in value)


def line_protocol(point):
    measurement = escape(point['measurement'], ' ,')
    tags = ''.join(f',{escape(key, " ,=")}={escape(value, " ,=")}'
                   for key, value in sorted(point['tags'].items()))
    fields = []
    for key, value in point['fields'].items():
        if type(value) is str:
            encoded = '"' + escape(value, '"') + '"'
        elif type(value) is bool:
            encoded = str(value).lower()
        elif type(value) is int:
            encoded = f'{value}i'
        else:
            encoded = repr(value)
        fields.append(f'{escape(key, " ,=")}={encoded}')
    return f'{measurement}{tags} {",".join(fields)} {point["timestamp_ns"]}'


def text_value(value, name):
    if not isinstance(value, str) or not value or any(c in value for c in '\r\n\0'):
        raise ValueError(f'Invalid {name}')
    return value


def topic_filter(value):
    text_value(value, 'topic filter')
    parts = value.split('/')
    if any(('+' in part and part != '+') or
           ('#' in part and (part != '#' or i != len(parts)-1))
           for i, part in enumerate(parts)):
        raise ValueError(f'Invalid MQTT filter: {value}')
    return value


def validate(cfg):
    names = set()
    for route in cfg['routes']:
        name = text_value(route['name'], 'rule name')
        if name in names:
            raise ValueError(f'Duplicate rule: {name}')
        names.add(name)
        pattern = topic_filter(route['topic'])
        captures = {f't{i}' for i in range(len(pattern.split('/'))) if '#' not in pattern.split('/')[:i+1]}
        for excluded in route.get('exclude_topics', []):
            topic_filter(excluded)
        for value in [route['measurement'], *route.get('tags', {}).values()]:
            text_value(value, 'measurement or tag')
            for _, field, spec, conversion in Formatter().parse(value):
                if field is not None and (field not in captures or spec or conversion):
                    raise ValueError(f'{name}: unknown topic placeholder {field}')
        fields = route.get('fields', [])
        if not isinstance(fields, list):
            raise ValueError(f'{name}: fields must be a list of MQTT field names')
        for field in fields:
            text_value(field, 'field name')
        for key in route.get('tags', {}):
            text_value(key, 'tag name')
        if not fields and not route.get('include_scalar_fields'):
            raise ValueError(f'{name}: missing fields')
    for pattern in cfg.get('observe', []):
        topic_filter(pattern)


def scalar_fields(data, names, recursive, depth=0):
    if depth > 7 or not isinstance(data, dict):
        return
    for key, value in data.items():
        if recursive and isinstance(value, dict):
            yield from scalar_fields(value, names, recursive, depth + 1)
        elif (not names or key in names) and key and not any(c in key for c in '\r\n\0'):
            if type(value) in (int, float):
                try:
                    number = float(value)
                except OverflowError:
                    continue
                if math.isfinite(number):
                    yield key, number
            elif type(value) is bool:
                yield key, value
            elif isinstance(value, str) and value and not any(c in value for c in '\r\n\0'):
                yield key, value


def points_for(routes, topic, data, retained, timestamp_ns):
    parts = {f't{i}': part for i, part in enumerate(topic.split('/'))}
    for route in routes:
        if not route.get('enabled', True) or not mqtt.topic_matches_sub(route['topic'], topic):
            continue
        if any(mqtt.topic_matches_sub(pattern, topic) for pattern in route.get('exclude_topics', [])):
            continue
        source = pointer(data, route.get('data_path', ''))
        if source is MISSING:
            continue
        names = () if route.get('include_scalar_fields') else route['fields']
        fields = dict(scalar_fields(source, names, route.get('recursive', False)))
        if not fields:
            continue
        point = dict(rule=route['name'], measurement=route['measurement'].format_map(parts),
                     tags={key: value.format_map(parts) for key, value in route.get('tags', {}).items()},
                     fields=fields, timestamp_ns=timestamp_ns)
        yield point, route.get('log', True) and (not retained or route.get('log_retained', False))


class Logger:
    def __init__(self, cfg, write):
        self.cfg = cfg
        self.write = write
        self.lock = Lock()
        self.latest = {}
        self.pending = Queue(maxsize=1024)
        filters = set(cfg.get('observe', [])) | {
            route['topic'] for route in cfg['routes'] if route.get('enabled', True)
        }
        self.subscriptions = {'#': 'pending'} if '#' in filters else dict.fromkeys(sorted(filters), 'pending')
        self.status = {'mqtt': 'connecting', 'mqtt_detail': '', 'influx': 'waiting' if write else 'preview',
                       'received': 0, 'last_received_at': None, 'written': 0, 'failed': 0, 'dropped': 0}

    def ingest(self, topic, payload, retained, timestamp_ns):
        if len(payload) > 65536:
            with self.lock:
                self.status['dropped'] += 1
            return
        raw = payload.decode('utf-8', errors='replace')
        try:
            data = json.loads(raw)
        except ValueError:
            data = raw
        timestamp = datetime.fromtimestamp(timestamp_ns / 1e9, timezone.utc).isoformat()
        with self.lock:
            self.status['received'] += 1
            self.status['last_received_at'] = timestamp
            destinations = {}
            for point, persist in points_for(self.cfg['routes'], topic, data, retained, timestamp_ns):
                key = (point['rule'], topic)
                previous = self.latest.get(key, {})
                fields = {**previous.get('fields', {}), **point['fields']}
                field_times = {**previous.get('field_times', {}),
                               **dict.fromkeys(point['fields'], timestamp)}
                self.latest[key] = {**point, 'fields': fields, 'field_times': field_times,
                                    'topic': topic, 'time': timestamp,
                                    'retained': retained}
                if len(self.latest) > 300:
                    del self.latest[next(iter(self.latest))]
                if self.write and persist:
                    series = point['measurement'], tuple(sorted(point['tags'].items()))
                    destination = destinations.setdefault(series, {**point, 'fields': {}})
                    destination['fields'].update(point['fields'])
            for destination in destinations.values():
                try:
                    self.pending.put_nowait(line_protocol(destination))
                except Full:
                    self.status['dropped'] += 1

    def snapshot(self):
        with self.lock:
            return dict(status=dict(self.status), latest=list(self.latest.values()),
                        subscriptions=dict(self.subscriptions),
                        broker={key: self.cfg['mqtt'][key] for key in ('host', 'port')},
                        database=self.cfg['influx']['database'], routes=self.cfg['routes'])

    def writer(self):
        cfg = self.cfg['influx']
        query = {'db': cfg['database'], 'precision': 'ns'}
        if cfg.get('retention_policy'):
            query['rp'] = cfg['retention_policy']
        headers = {'Content-Type': 'text/plain'}
        if cfg.get('username'):
            credentials = f"{cfg['username']}:{cfg['password']}".encode()
            headers['Authorization'] = 'Basic ' + b64encode(credentials).decode()
        url = f"{cfg['url'].rstrip('/')}/write?{urlencode(query)}"
        while True:
            line = self.pending.get()
            request = Request(url, data=line.encode(), headers=headers)
            try:
                with urlopen(request, timeout=5) as response:
                    if response.status != 204:
                        raise OSError(f'HTTP {response.status}')
                state, counter = 'ok', 'written'
            except OSError as error:
                state, counter = f'error ({getattr(error, "code", type(error).__name__)})', 'failed'
            with self.lock:
                self.status['influx'] = state
                self.status[counter] += 1


    def client(self):
        cfg = self.cfg['mqtt']
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv311)
        if cfg.get('username'):
            password = cfg.get('password')
            if not password:
                raise ValueError('Set the MQTT password in mqtt.password in config.json')
            client.username_pw_set(cfg['username'], password)
        if cfg.get('tls'):
            client.tls_set(ca_certs=cfg.get('ca_file'))
        filters = list(self.subscriptions)

        def connection_state(state, detail=''):
            with self.lock:
                self.status.update(mqtt=state, mqtt_detail=detail)
                self.subscriptions = dict.fromkeys(filters, 'pending')

        def connected(client, userdata, flags, reason, properties):
            if reason.is_failure:
                connection_state('error', str(reason))
                return
            connection_state('connected')
            if filters:
                result, _ = client.subscribe([(pattern, 1) for pattern in filters])
                if result != mqtt.MQTT_ERR_SUCCESS:
                    with self.lock:
                        self.subscriptions = dict.fromkeys(filters, 'rejected')
                        self.status['mqtt_detail'] = mqtt.error_string(result)

        def subscribed(client, userdata, mid, reasons, properties):
            with self.lock:
                for pattern, reason in zip(filters, reasons):
                    self.subscriptions[pattern] = 'rejected' if reason.is_failure else str(reason)

        def disconnected(client, userdata, flags, reason, properties):
            connection_state('disconnected', str(reason))

        def connect_failed(client, userdata):
            connection_state('error', 'Broker unreachable · retrying automatically')

        def message(client, userdata, packet):
            self.ingest(packet.topic, packet.payload, packet.retain,
                        time_ns())
        client.on_connect = connected
        client.on_connect_fail = connect_failed
        client.on_subscribe = subscribed
        client.on_disconnect = disconnected
        client.on_message = message
        client.connect_async(cfg['host'], cfg['port'])
        return client


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=BASE / 'config.json')
    parser.add_argument('--check', action='store_true', help='Validate rules and exit')
    args = parser.parse_args()
    cfg = json.loads(args.config.read_text())
    validate(cfg)
    influx = cfg['influx']
    write = influx.get('enabled', False)
    if cfg['mqtt'].get('username') and not cfg['mqtt'].get('password'):
        raise ValueError('Set mqtt.password in config.json')
    if write and influx.get('username') and not influx.get('password'):
        raise ValueError('Set influx.password in config.json')
    if args.check:
        print(f"OK: {len(cfg['routes'])} rules; InfluxDB writes {'enabled' if write else 'disabled'}")
        return
    logger = Logger(cfg, write)
    page = (BASE / 'index.html').read_bytes()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/':
                body, kind = page, 'text/html; charset=utf-8'
            elif self.path == '/api/state':
                body, kind = json.dumps(logger.snapshot(), allow_nan=False).encode(), 'application/json'
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header('Content-Type', kind)
            self.send_header('Cache-Control', 'no-store')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        def log_message(self, *_):
            pass

    web = cfg['web']
    with ThreadingHTTPServer((web['host'], web['port']), Handler) as server:
        client = logger.client()
        if write:
            Thread(target=logger.writer, daemon=True).start()
        client.loop_start()
        try:
            for url in dashboard_urls(web['host'], server.server_port, cfg['mqtt']):
                print(f'MQTT Monitor: {url}', flush=True)
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            client.disconnect()
            client.loop_stop()


if __name__ == '__main__':
    main()
