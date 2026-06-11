import sys
import types
import unittest

try:
    import paho.mqtt.client  # noqa: F401
except ModuleNotFoundError:
    paho_package = types.ModuleType("paho")
    paho_package.__path__ = []
    mqtt_package = types.ModuleType("paho.mqtt")
    mqtt_package.__path__ = []
    mqtt_client_module = types.ModuleType("paho.mqtt.client")
    sys.modules.update({
        "paho": paho_package,
        "paho.mqtt": mqtt_package,
        "paho.mqtt.client": mqtt_client_module,
    })

try:
    import requests  # noqa: F401
except ModuleNotFoundError:
    sys.modules["requests"] = types.ModuleType("requests")

import run


class FakeMqttClient:
    def __init__(self):
        self.calls = []

    def reconnect_delay_set(self, min_delay, max_delay):
        self.calls.append(("reconnect_delay_set", min_delay, max_delay))

    def connect_async(self, host, port, keepalive):
        self.calls.append(("connect_async", host, port, keepalive))

    def loop_start(self):
        self.calls.append(("loop_start",))


class MqttStartupTests(unittest.TestCase):
    def test_start_mqtt_loop_uses_non_blocking_connection_with_backoff(self):
        client = FakeMqttClient()

        run.start_mqtt_loop(client, "mqtt.example", 1883)

        self.assertEqual(
            client.calls,
            [
                ("reconnect_delay_set", 1, 120),
                ("connect_async", "mqtt.example", 1883, 60),
                ("loop_start",),
            ],
        )

    def test_disconnect_callback_accepts_v1_signature(self):
        run.on_disconnect(None, None, 0)

    def test_disconnect_callback_accepts_v2_signature(self):
        run.on_disconnect(None, None, {}, 0, None)


if __name__ == "__main__":
    unittest.main()
