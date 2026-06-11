import sys
import types
import unittest
import json
import logging
from unittest.mock import patch

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

    def publish(self, topic, payload, retain=False):
        self.calls.append(("publish", topic, payload, retain))


class FakePdu:
    def __init__(self, status):
        self.host = "192.0.2.10"
        self._status = status

    def status(self):
        return self._status


class MqttStartupTests(unittest.TestCase):
    def test_log_level_normalization_supports_configured_levels(self):
        self.assertEqual(run.normalize_log_level("warning"), "WARNING")
        self.assertEqual(run.normalize_log_level("INFO"), "INFO")
        self.assertEqual(run.normalize_log_level("Debug"), "DEBUG")
        self.assertEqual(run.normalize_log_level("verbose"), "INFO")

    def test_configure_logging_applies_selected_threshold(self):
        werkzeug_logger = logging.getLogger("werkzeug")
        urllib3_logger = logging.getLogger("urllib3")
        old_werkzeug_level = werkzeug_logger.level
        old_urllib3_level = urllib3_logger.level
        try:
            with patch.object(run.logging, "basicConfig") as basic_config:
                selected = run.configure_logging("DEBUG")

            self.assertEqual(selected, "DEBUG")
            self.assertEqual(basic_config.call_args.kwargs["level"], logging.DEBUG)
            self.assertEqual(werkzeug_logger.level, logging.INFO)
            self.assertEqual(urllib3_logger.level, logging.WARNING)
        finally:
            werkzeug_logger.setLevel(old_werkzeug_level)
            urllib3_logger.setLevel(old_urllib3_level)

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

    def test_publish_status_splits_device_info_into_scalar_topics(self):
        client = FakeMqttClient()
        old_client, old_topic = run.client, run.mqtt_topic
        run.client, run.mqtt_topic = client, "pdu"
        try:
            run.publish_status("rack", FakePdu({"outlets": []}))
        finally:
            run.client, run.mqtt_topic = old_client, old_topic

        self.assertIn(
            ("publish", "pdu/rack/device/model", "LogiLink PDU8P01", True),
            client.calls,
        )
        self.assertIn(
            ("publish", "pdu/rack/device/ip", "192.0.2.10", True),
            client.calls,
        )
        self.assertIn(
            ("publish", "pdu/rack/device/status", "online", True),
            client.calls,
        )
        self.assertNotIn("pdu/rack/device/info", [call[1] for call in client.calls])

    def test_discovery_replaces_combined_device_info_sensor(self):
        client = FakeMqttClient()
        old_client = run.client
        old_topic = run.mqtt_topic
        old_instances = run.pdu_instances
        run.client = client
        run.mqtt_topic = "pdu"
        run.pdu_instances = {"rack": FakePdu({"outlets": []})}
        try:
            run.send_discovery_messages()
        finally:
            run.client = old_client
            run.mqtt_topic = old_topic
            run.pdu_instances = old_instances

        publishes = {
            call[1]: call[2]
            for call in client.calls
            if call[0] == "publish"
        }
        self.assertEqual(
            publishes["homeassistant/sensor/rack_device_info/config"],
            "",
        )
        self.assertEqual(publishes["pdu/rack/device/info"], "")
        for sensor_id in ("model", "ip", "status"):
            topic = f"homeassistant/sensor/rack_{sensor_id}/config"
            config = json.loads(publishes[topic])
            self.assertEqual(
                config["state_topic"],
                f"pdu/rack/device/{sensor_id}",
            )


if __name__ == "__main__":
    unittest.main()
