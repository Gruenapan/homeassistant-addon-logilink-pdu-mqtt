#!/usr/bin/env python3
"""
PDU MQTT Bridge - Complete version with all features
"""

import time
import os
import json
import paho.mqtt.client as mqtt
import logging
import sys
import threading
import requests
from pdu import PDU
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Global variables
client = None
mqtt_topic = None
pdu_instances = {}
PDU_MODEL = "LogiLink PDU8P01"

def as_bool(value: Any, default: bool = False) -> bool:
    """Convert string/bool-ish values to bool."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ('1', 'true', 'yes', 'on')
    if value is None:
        return default
    return bool(value)

def load_config():
    """Load configuration from Home Assistant add-on options"""
    def env_int(name: str, default: int) -> int:
        try:
            return int(os.getenv(name, default))
        except (TypeError, ValueError):
            return default

    try:
        with open('/data/options.json', 'r') as f:
            options = json.load(f)
        logger.info("Loaded configuration from Home Assistant options")
        return options
    except FileNotFoundError:
        logger.warning("Home Assistant options file not found, using environment variables")
        raw_device_list = os.getenv('DEVICE_LIST', os.getenv('PDU_LIST', '[]'))
        try:
            parsed_device_list = json.loads(raw_device_list)
            if not isinstance(parsed_device_list, list):
                parsed_device_list = []
        except json.JSONDecodeError:
            logger.warning("Invalid DEVICE_LIST/PDU_LIST JSON, using empty list")
            parsed_device_list = []
        return {
            'mqtt_host': os.getenv('MQTT_HOST', 'localhost'),
            'mqtt_port': env_int('MQTT_PORT', 1883),
            'mqtt_user': os.getenv('MQTT_USER', ''),
            'mqtt_password': os.getenv('MQTT_PASSWORD', ''),
            'mqtt_topic': os.getenv('MQTT_TOPIC', 'pdu'),
            # Keep both keys for backward compatibility with existing configs/docs
            'device_list': parsed_device_list,
            'pdu_list': parsed_device_list,
            'auto_discovery': os.getenv('AUTO_DISCOVERY', 'false').lower() == 'true',
            'discovery_network': os.getenv('DISCOVERY_NETWORK', '192.168.1'),
            'discovery_range_start': env_int('DISCOVERY_RANGE_START', 1),
            'discovery_range_end': env_int('DISCOVERY_RANGE_END', 254)
        }

def normalize_pdu_list(config: Dict[str, Any]) -> list:
    """
    Normalize PDU configuration from both new and legacy keys.
    Supports:
      - device_list (current add-on schema)
      - pdu_list (legacy docs/format)
    """
    raw_devices = []
    if isinstance(config.get('device_list'), list):
        raw_devices.extend(config.get('device_list', []))
    if isinstance(config.get('pdu_list'), list):
        raw_devices.extend(config.get('pdu_list', []))

    pdus = []
    seen = set()
    for idx, device in enumerate(raw_devices, start=1):
        if not isinstance(device, dict):
            logger.warning(f"Ignoring invalid device entry #{idx}: expected object")
            continue

        device_type = str(device.get('type', 'PDU')).strip().lower()
        if device_type and device_type not in ('pdu', 'logilink', 'intellinet', 'pdu8p01'):
            logger.debug(
                "Skipping non-PDU device '%s' (type=%s)",
                device.get('name', f"entry_{idx}"),
                device_type
            )
            continue

        host = str(device.get('host', device.get('ip', ''))).strip()
        if not host:
            logger.warning(f"Ignoring device entry without host: {device}")
            continue

        name = str(device.get('name', f"pdu_{host.replace('.', '_')}")).strip()
        username = str(device.get('username', 'admin'))
        password = str(device.get('password', 'admin'))

        dedupe_key = (name, host)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        pdus.append({
            'name': name,
            'host': host,
            'username': username,
            'password': password
        })

    return pdus

def probe_pdu_status(host: str, username: str, password: str, timeout: float = 1.5) -> bool:
    """Quick probe used by network auto-discovery."""
    try:
        response = requests.get(
            f"http://{host}/status.xml",
            auth=(username, password),
            timeout=timeout
        )
        return response.status_code == 200 and "<response>" in response.text
    except requests.RequestException:
        return False

def discover_pdus_from_network(config: Dict[str, Any], existing_pdus: list) -> list:
    """Discover PDUs on network when none are explicitly configured."""
    network = str(config.get('discovery_network', '')).strip()
    if not network:
        logger.warning("Auto-discovery enabled but discovery_network is empty")
        return []

    try:
        start_ip = max(1, min(int(config.get('discovery_range_start', 1)), 254))
        end_ip = max(1, min(int(config.get('discovery_range_end', 254)), 254))
        if start_ip > end_ip:
            start_ip, end_ip = end_ip, start_ip
    except (TypeError, ValueError):
        logger.warning("Invalid discovery range, using 1-254")
        start_ip, end_ip = 1, 254

    existing_hosts = {pdu['host'] for pdu in existing_pdus if 'host' in pdu}
    hosts_to_probe = [
        f"{network}.{ip}"
        for ip in range(start_ip, end_ip + 1)
        if f"{network}.{ip}" not in existing_hosts
    ]

    # Try common default credentials first
    credentials = [
        ('admin', 'admin'),
        ('admin', ''),
        ('root', 'admin'),
        ('user', 'user')
    ]

    logger.info(
        f"Starting network PDU auto-discovery on {network}.{start_ip}-{end_ip} ({len(hosts_to_probe)} hosts)"
    )

    discovered = []
    max_workers = min(64, max(1, len(hosts_to_probe)))

    def check_host(host: str):
        for username, password in credentials:
            if probe_pdu_status(host, username, password):
                return {
                    'name': f"pdu_{host.replace('.', '_')}",
                    'host': host,
                    'username': username or 'admin',
                    'password': password
                }
        return None

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(check_host, host): host for host in hosts_to_probe}
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    discovered.append(result)
                    logger.info(
                        f"Auto-discovery found PDU at {result['host']} (username={result['username']})"
                    )
            except Exception as e:
                logger.debug(f"Auto-discovery probe error: {e}")

    logger.info(f"Auto-discovery completed. Found {len(discovered)} PDU(s).")
    return discovered

def on_connect(client, userdata, flags, rc, properties=None):
    """MQTT connection callback (compatible with both API versions)"""
    # Handle both API v1 and v2 (properties parameter is optional in v1)
    rc_value = int(rc) if hasattr(rc, "__int__") else rc
    if rc_value == 0:
        logger.info("Connected to MQTT broker")
        
        # Subscribe to control topics for all PDUs
        for pdu_name in pdu_instances.keys():
            base = f"{mqtt_topic}/{pdu_name}"
            
            # Basic outlet control
            for i in range(1, 9):
                client.subscribe(f"{base}/outlet{i}/set")
                logger.info(f"Subscribed to {base}/outlet{i}/set")
            
            # Extended configuration topics
            client.subscribe(f"{base}/config/+/set")
            client.subscribe(f"{base}/network/set")
            client.subscribe(f"{base}/threshold/+/set")
            client.subscribe(f"{base}/outlet/+/config/set")
            client.subscribe(f"{base}/system/reboot")
            client.subscribe(f"{base}/snmp/set")
            client.subscribe(f"{base}/email/set")
            
            logger.info(f"Subscribed to all control topics for {pdu_name}")
        
        # Send MQTT Discovery messages
        send_discovery_messages()
    else:
        logger.error(f"Failed to connect to MQTT broker: {rc}")

def on_disconnect(client, userdata, disconnect_flags, reason_code=None, properties=None):
    """MQTT disconnection callback compatible with Paho API v1 and v2."""
    # API v1 passes rc as the third argument. API v2 adds disconnect flags
    # before the reason code.
    rc = disconnect_flags if reason_code is None else reason_code
    if rc != 0:
        logger.warning(f"Unexpected disconnect from MQTT broker: {rc}")

def start_mqtt_loop(mqtt_client, mqtt_host, mqtt_port):
    """Start a non-blocking MQTT connection with automatic retry backoff."""
    logger.info(f"Connecting to MQTT broker at {mqtt_host}:{mqtt_port}")
    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=120)
    mqtt_client.connect_async(mqtt_host, mqtt_port, 60)
    mqtt_client.loop_start()

def on_message(client, userdata, msg):
    """Handle incoming MQTT messages"""
    try:
        topic_parts = msg.topic.split('/')
        if len(topic_parts) < 3:
            return
            
        pdu_name = topic_parts[1]
        if pdu_name not in pdu_instances:
            logger.error(f"Unknown PDU: {pdu_name}")
            return
            
        pdu = pdu_instances[pdu_name]
        payload = msg.payload.decode('utf-8')
        
        # Basic outlet control
        if topic_parts[2].startswith('outlet') and len(topic_parts) > 3 and topic_parts[3] == 'set':
            outlet_num = int(topic_parts[2].replace('outlet', ''))
            state = payload.upper() == 'ON'
            if pdu.set_outlet(outlet_num, state):
                logger.info(f"Set {pdu_name} outlet {outlet_num} to {payload}")
                client.publish(f"{mqtt_topic}/{pdu_name}/outlet{outlet_num}/state", 
                             payload.upper(), retain=True)
            else:
                logger.error(f"Failed to set {pdu_name} outlet {outlet_num}")
                
        # Extended features (for future implementation)
        elif topic_parts[2] == 'outlet' and len(topic_parts) > 5 and topic_parts[4] == 'config' and topic_parts[5] == 'set':
            outlet_num = int(topic_parts[3])
            logger.info(f"Outlet config request for {pdu_name} outlet {outlet_num}: {payload}")
            # TODO: Implement outlet configuration when PDU supports it
            
        elif topic_parts[2] == 'network' and len(topic_parts) > 3 and topic_parts[3] == 'set':
            logger.info(f"Network config request for {pdu_name}: {payload}")
            # TODO: Implement network configuration when PDU supports it
            
        elif topic_parts[2] == 'threshold' and len(topic_parts) > 4 and topic_parts[4] == 'set':
            sensor_type = topic_parts[3]
            logger.info(f"Threshold config request for {pdu_name} {sensor_type}: {payload}")
            # TODO: Implement threshold configuration when PDU supports it
            
        elif topic_parts[2] == 'system' and len(topic_parts) > 3 and topic_parts[3] == 'reboot':
            if payload.upper() == 'REBOOT':
                logger.warning(f"Reboot request for {pdu_name}")
                # TODO: Implement reboot when PDU supports it
                
    except Exception as e:
        logger.error(f"Error handling message on {msg.topic}: {e}")

def publish_status(pdu_name, pdu):
    """Publish status for all outlets of a PDU"""
    try:
        logger.debug(f"Publishing status for PDU: {pdu_name}")
        status = pdu.status()

        device_info = {
            "model": PDU_MODEL,
            "ip": pdu.host,
            "status": "online" if status else "offline"
        }
        for info_id, value in device_info.items():
            client.publish(
                f"{mqtt_topic}/{pdu_name}/device/{info_id}",
                value,
                retain=True
            )

        if status:
            # Publish outlet states
            if 'outlets' in status:
                for i, state in enumerate(status['outlets']):
                    outlet_num = i + 1
                    state_topic = f"{mqtt_topic}/{pdu_name}/outlet{outlet_num}/state"
                    mqtt_state = "ON" if state == 'on' else "OFF"
                    client.publish(state_topic, mqtt_state, retain=True)
                    logger.debug(f"Published {state_topic} = {mqtt_state}")
            # Publish sensor data
            if 'tempBan' in status and status['tempBan']:
                temp_topic = f"{mqtt_topic}/{pdu_name}/sensor/temperature"
                client.publish(temp_topic, status['tempBan'], retain=True)
            if 'humBan' in status and status['humBan']:
                hum_topic = f"{mqtt_topic}/{pdu_name}/sensor/humidity"
                client.publish(hum_topic, status['humBan'], retain=True)
            if 'curBan' in status and status['curBan']:
                cur_topic = f"{mqtt_topic}/{pdu_name}/sensor/current"
                client.publish(cur_topic, status['curBan'], retain=True)
            logger.debug(f"Status published for PDU {pdu_name} - {len(status.get('outlets', []))} outlets")
        else:
            logger.warning(f"No status data received from PDU: {pdu_name}")
    except Exception as e:
        logger.error(f"Error publishing status for {pdu_name}: {e}")

def send_discovery_messages():
    """Send Home Assistant MQTT Discovery messages"""
    discovery_prefix = "homeassistant"
    
    for pdu_name in pdu_instances.keys():
        logger.info(f"Sending discovery messages for {pdu_name}")
        # Remove prefix 'pdu_' se existir
        clean_name = pdu_name
        if clean_name.startswith("pdu_"):
            clean_name = clean_name[4:]
        # Create discovery for each outlet switch
        for i in range(1, 9):
            entity_id = f"{clean_name}_outlet{i}"
            switch_config = {
                "name": f"Outlet {i}",
                "unique_id": entity_id,
                "object_id": entity_id,
                "command_topic": f"{mqtt_topic}/{pdu_name}/outlet{i}/set",
                "state_topic": f"{mqtt_topic}/{pdu_name}/outlet{i}/state",
                "payload_on": "ON",
                "payload_off": "OFF",
                "device_class": "outlet",
                "device": {
                    "identifiers": [f"pdu_{pdu_name}"],
                    "name": f"PDU {pdu_name}",
                    "model": "LogiLink PDU8P01",
                    "manufacturer": "LogiLink"
                }
            }
            discovery_topic = f"{discovery_prefix}/switch/{entity_id}/config"
            client.publish(discovery_topic, json.dumps(switch_config), retain=True)
            logger.debug(f"Published discovery for switch.{entity_id}")
        # Create discovery for sensors
        sensors = [
            ("temperature", "Temperature", "°C", "temperature"),
            ("humidity", "Humidity", "%", "humidity"),
            ("current", "Current", "A", "current")
        ]
        for sensor_id, name, unit, device_class in sensors:
            sensor_entity_id = f"{clean_name}_{sensor_id}"
            sensor_config = {
                "name": f"{name}",
                "unique_id": sensor_entity_id,
                "object_id": sensor_entity_id,
                "state_topic": f"{mqtt_topic}/{pdu_name}/sensor/{sensor_id}",
                "unit_of_measurement": unit,
                "device_class": device_class,
                "device": {
                    "identifiers": [f"pdu_{pdu_name}"],
                    "name": f"PDU {pdu_name}",
                    "model": "LogiLink PDU8P01",
                    "manufacturer": "LogiLink"
                }
            }
            discovery_topic = f"{discovery_prefix}/sensor/{sensor_entity_id}/config"
            client.publish(discovery_topic, json.dumps(sensor_config), retain=True)
            logger.debug(f"Published discovery for sensor.{sensor_entity_id}")
        # Remove the legacy combined JSON sensor and its retained state.
        client.publish(
            f"{discovery_prefix}/sensor/{clean_name}_device_info/config",
            "",
            retain=True
        )
        client.publish(f"{mqtt_topic}/{pdu_name}/device/info", "", retain=True)

        device_info_sensors = [
            ("model", "Model", "mdi:information-outline"),
            ("ip", "IP", "mdi:ip-network"),
            ("status", "Status", "mdi:lan-connect")
        ]
        for sensor_id, name, icon in device_info_sensors:
            sensor_entity_id = f"{clean_name}_{sensor_id}"
            sensor_config = {
                "name": name,
                "unique_id": sensor_entity_id,
                "object_id": sensor_entity_id,
                "state_topic": f"{mqtt_topic}/{pdu_name}/device/{sensor_id}",
                "icon": icon,
                "entity_category": "diagnostic",
                "device": {
                    "identifiers": [f"pdu_{pdu_name}"],
                    "name": f"PDU {pdu_name}",
                    "model": PDU_MODEL,
                    "manufacturer": "LogiLink"
                }
            }
            discovery_topic = f"{discovery_prefix}/sensor/{sensor_entity_id}/config"
            client.publish(discovery_topic, json.dumps(sensor_config), retain=True)
    logger.info("MQTT Discovery messages sent")

def main():
    global client, mqtt_topic, pdu_instances
    
    try:
        # Load configuration
        config = load_config()
        mqtt_host = config.get('mqtt_host', 'localhost')
        mqtt_port = config.get('mqtt_port', 1883)
        mqtt_user = config.get('mqtt_user', '')
        mqtt_password = config.get('mqtt_password', '')
        mqtt_topic = config.get('mqtt_topic', 'pdu')
        auto_discovery = as_bool(config.get('auto_discovery', False), default=False)

        # Support both `device_list` (current schema) and `pdu_list` (legacy)
        pdu_list = normalize_pdu_list(config)

        # Auto-discover PDUs only when no explicit PDU configuration exists
        if not pdu_list and auto_discovery:
            logger.info("No PDUs configured in device_list/pdu_list. Trying network auto-discovery...")
            pdu_list = discover_pdus_from_network(config, pdu_list)
        
        # Start web interface in background
        logger.info("Starting PDU Discovery Web Interface...")
        web_thread = threading.Thread(target=start_web_interface, daemon=True)
        web_thread.start()
        
        if not pdu_list:
            logger.warning(
                "No PDUs configured/discovered yet - use device_list in add-on options or the web interface."
            )
            logger.info("Web interface available at: http://localhost:8099")
            # Keep running even without PDUs for web interface
            logger.info("Entering standby mode - waiting for PDU configuration...")
            while True:
                logger.debug("Standby mode - sleeping for 60 seconds...")
                time.sleep(60)
            return
            
        # Create PDU instances
        for pdu_config in pdu_list:
            pdu_name = pdu_config['name']
            pdu_instances[pdu_name] = PDU(
                pdu_config['host'],
                pdu_config.get('username', 'admin'),
                pdu_config.get('password', 'admin')
            )
            logger.info(f"Created PDU instance for {pdu_name}")
        
        logger.info("Starting PDU MQTT Bridge v1.4.3")
        logger.info(f"MQTT: {mqtt_host}:{mqtt_port}")
        logger.info(f"PDUs: {list(pdu_instances.keys())}")
        logger.info(f"Web interface: http://localhost:8099")
        
        # Setup MQTT client with version compatibility
        try:
            # Try new API (paho-mqtt >= 2.0)
            client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
            logger.debug("Using MQTT Client API v2")
        except AttributeError:
            # Fallback to old API (paho-mqtt < 2.0)
            client = mqtt.Client()
            logger.debug("Using MQTT Client API v1 (legacy)")
        
        if mqtt_user:
            client.username_pw_set(mqtt_user, mqtt_password)
            
        client.on_connect = on_connect
        client.on_message = on_message
        client.on_disconnect = on_disconnect
        
        # Keep the add-on alive while the MQTT broker is unavailable. Paho's
        # network loop retries the asynchronous initial connection and later
        # disconnects using the configured backoff.
        start_mqtt_loop(client, mqtt_host, mqtt_port)
        
        # Wait for connection
        time.sleep(2)
        
        # Main loop
        while True:
            try:
                for pdu_name, pdu in pdu_instances.items():
                    try:
                        publish_status(pdu_name, pdu)
                    except Exception as e:
                        logger.error(f"Error updating {pdu_name}: {e}")
                
                import datetime
                current_time = datetime.datetime.now().strftime("%H:%M:%S")
                logger.info(f"Main loop completed at {current_time}, sleeping for 30 seconds...")
                time.sleep(30)  # Update every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(30)  # Sleep even on error to prevent tight loop
            
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Application error: {e}")
    finally:
        if client:
            client.loop_stop()
            client.disconnect()

def start_web_interface():
    """Start the web interface for PDU discovery"""
    try:
        import os
        import sys
        
        # Check if web_interface.py exists
        if not os.path.exists('web_interface.py'):
            logger.warning("web_interface.py not found - web interface disabled")
            return
        
        # Check if flask is available
        try:
            import flask
        except ImportError:
            logger.warning("Flask not available - web interface disabled")
            return
        
        # Try to import and run web interface
        from web_interface import run_server
        logger.info("Starting web interface on port 8099...")
        run_server()
        
    except ImportError as e:
        logger.warning(f"Web interface module not available: {e}")
        logger.info("Continuing without web interface...")
    except Exception as e:
        logger.error(f"Failed to start web interface: {e}")
        logger.info("Continuing without web interface...")

if __name__ == "__main__":
    main()
