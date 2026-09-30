#!/usr/bin/env python3
"""Publish local clock to MQTT yard/time every 30s for the Feather.

    python3 yard_time_pub.py
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone, timedelta

import paho.mqtt.client as mqtt

MQTT_HOST = os.environ.get("MQTT_HOST", "YOUR_MQTT_HOST")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER", "YOUR_MQTT_USER")
MQTT_PASS = os.environ.get("MQTT_PASSWORD", os.environ.get("MQTT_PASS", "YOUR_MQTT_PASSWORD"))
TOPIC = "yard/time"
KST = timezone(timedelta(hours=9))


def main() -> None:
    ver = getattr(mqtt, "CallbackAPIVersion", None)
    if ver is not None:
        c = mqtt.Client(ver.VERSION2, client_id="yard-time")
    else:
        c = mqtt.Client(client_id="yard-time")
    c.username_pw_set(MQTT_USER, MQTT_PASS)
    c.connect(MQTT_HOST, MQTT_PORT, 30)
    print("pub", TOPIC, "->", MQTT_HOST)
    while True:
        now = time.time()
        epoch = int(now)
        kst = datetime.fromtimestamp(now, KST).strftime("%Y-%m-%d %H:%M:%S")
        payload = json.dumps({"epoch": epoch, "kst": kst})
        c.publish(TOPIC, payload, retain=True)
        c.loop(timeout=1.0)
        print(payload)
        time.sleep(30)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nstop")
