"""
MQTT client for receiving and parsing weather station sensor data.
Subscribes to a single topic, validates JSON payloads, and forwards
parsed WeatherSample objects via callback.
"""
import json
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Callable, Optional

import paho.mqtt.client as mqtt

from app.models import WeatherSample
from app.config import MQTTConfig, config


logger = logging.getLogger(__name__)


class MQTTClient:
    """
    MQTT client that subscribes to weather sensor data, parses JSON payloads,
    and invokes a callback with validated WeatherSample objects.
    """
    
    def __init__(
        self,
        config: MQTTConfig,
        on_sample_received: Callable[[WeatherSample], None],
    ):
        """
        Initialize the MQTT client.
        
        Args:
            config: MQTT connection configuration
            on_sample_received: Callback function invoked with each parsed WeatherSample
        """
        self.config = config
        self.on_sample_received = on_sample_received
        
        # Create paho MQTT client
        self.client = mqtt.Client(
            client_id=config.client_id,
            protocol=mqtt.MQTTv311,
        )
        
        # Set callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        
        # Optional authentication
        if config.username and config.password:
            self.client.username_pw_set(config.username, config.password)
            logger.info("MQTT authentication configured")
        
        self._is_connected = False
    
    def connect(self) -> None:
        """
        Connect to the MQTT broker.
        
        Raises:
            ConnectionError: If connection fails
        """
        try:
            logger.info(f"Connecting to MQTT broker at {self.config.host}:{self.config.port}")
            self.client.connect(
                host=self.config.host,
                port=self.config.port,
                keepalive=self.config.keepalive,
            )
            logger.info("MQTT connection initiated")
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            raise ConnectionError(f"MQTT connection failed: {e}") from e
    
    def start(self) -> None:
        """
        Start the MQTT client loop in a background thread.
        Non-blocking call.
        """
        logger.info("Starting MQTT client loop")
        self.client.loop_start()
    
    def stop(self) -> None:
        """
        Stop the MQTT client loop and disconnect gracefully.
        """
        logger.info("Stopping MQTT client")
        self.client.loop_stop()
        self.client.disconnect()
        self._is_connected = False
        logger.info("MQTT client stopped")
    
    def is_connected(self) -> bool:
        """Check if the client is currently connected to the broker."""
        return self._is_connected
    
    # -------------------------------------------------------------------------
    # Paho MQTT Callbacks
    # -------------------------------------------------------------------------
    
    def _on_connect(
        self,
        client: mqtt.Client,
        userdata,
        flags,
        rc: int,
    ) -> None:
        """
        Callback invoked when the client connects to the broker.
        
        Args:
            rc: Connection result code (0 = success)
        """
        if rc == 0:
            logger.info(f"Connected to MQTT broker successfully")
            self._is_connected = True
            
            # Subscribe to the configured topic
            result, mid = client.subscribe(self.config.topic, qos=self.config.qos)
            
            if result == mqtt.MQTT_ERR_SUCCESS:
                logger.info(f"Subscribed to topic: {self.config.topic} (QoS={self.config.qos})")
            else:
                logger.error(f"Failed to subscribe to topic: {self.config.topic}")
        else:
            logger.error(f"MQTT connection failed with code {rc}: {mqtt.connack_string(rc)}")
            self._is_connected = False
    
    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata,
        rc: int,
    ) -> None:
        """
        Callback invoked when the client disconnects from the broker.
        
        Args:
            rc: Disconnection result code (0 = clean disconnect)
        """
        self._is_connected = False
        
        if rc == 0:
            logger.info("Disconnected from MQTT broker cleanly")
        else:
            logger.warning(f"Unexpected disconnection from MQTT broker (code {rc})")
    
    def _on_message(
        self,
        client: mqtt.Client,
        userdata,
        msg: mqtt.MQTTMessage,
    ) -> None:
        """
        Callback invoked when a message is received on a subscribed topic.
        Parses the JSON payload and forwards it to the registered callback.
        
        Args:
            msg: The received MQTT message
        """
        try:
            logger.debug(f"Received message on topic {msg.topic}")
            
            # Parse JSON payload
            payload = self._parse_json(msg.payload)
            
            # Convert to WeatherSample
            sample = self._json_to_weather_sample(payload)
            
            # Forward to callback
            self.on_sample_received(sample)
            
            logger.debug(f"Successfully processed message: {sample}")
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in MQTT message: {e}")
            logger.debug(f"Raw payload: {msg.payload}")
        
        except (KeyError, TypeError, ValueError) as e:
            logger.error(f"Failed to parse weather sample: {e}")
            logger.debug(f"Payload: {msg.payload}")
        
        except Exception as e:
            logger.exception(f"Unexpected error processing MQTT message: {e}")
    
    # -------------------------------------------------------------------------
    # JSON Parsing and Validation
    # -------------------------------------------------------------------------
    
    @staticmethod
    def _parse_json(payload: bytes) -> dict:
        """
        Parse JSON payload from MQTT message.
        
        Args:
            payload: Raw bytes from MQTT message
            
        Returns:
            Parsed JSON as dictionary
            
        Raises:
            json.JSONDecodeError: If payload is not valid JSON
        """
        return json.loads(payload.decode("utf-8"))
    
    def _json_to_weather_sample(self, data: dict) -> WeatherSample:
        """
        Convert a JSON dictionary to a WeatherSample object.
        
        Expected JSON structure:
        {
            "temperature": 23.5,
            "humidity": 65.2,
            "co2": 450.0,
            "flammable_gas": 120.5,
            "toxic_gas": 85.3,
            "uv_index": 5.2,
            "battery": 3.7,
            "latitude": -23.550520,
            "longitude": -46.633308,
            "altitude": 760.0,
            "satellites": 8,
            "fix_quality": 1
        }
        
        Args:
            data: Parsed JSON dictionary
            
        Returns:
            WeatherSample object with server-side timestamp
            
        Raises:
            KeyError: If required fields are missing
            ValueError: If data validation fails
        """
        
        try:
            tz = ZoneInfo(config.timezone)
        except Exception:
            # Fallback to UTC if the timezone string is invalid
            logger.warning(f"Invalid timezone '{config.timezone}', falling back to UTC")
            tz = ZoneInfo("UTC")

        timestamp = datetime.now(tz)
        
        # Required fields
        temperature_c = float(data["temperature"])
        humidity_pct = float(data["humidity"])
        air_quality_co2_ppm = float(data["co2"])
        flammable_gas_ppm = float(data["flammable_gas"])
        toxic_gas_ppm = float(data["toxic_gas"])
        uv_index = float(data["uv_index"])
        battery_voltage = float(data["battery"])
        
        # Optional GPS fields (may be null or missing)
        gps_latitude = self._get_optional_float(data, "latitude")
        gps_longitude = self._get_optional_float(data, "longitude")
        gps_altitude_m = self._get_optional_float(data, "altitude")
        gps_satellites = self._get_optional_int(data, "satellites")
        gps_fix_quality = self._get_optional_int(data, "fix_quality")
        
        return WeatherSample(
            timestamp=timestamp,
            temperature_c=temperature_c,
            humidity_pct=humidity_pct,
            air_quality_co2_ppm=air_quality_co2_ppm,
            flammable_gas_ppm=flammable_gas_ppm,
            toxic_gas_ppm=toxic_gas_ppm,
            uv_index=uv_index,
            battery_voltage=battery_voltage,
            gps_latitude=gps_latitude,
            gps_longitude=gps_longitude,
            gps_altitude_m=gps_altitude_m,
            gps_satellites=gps_satellites,
            gps_fix_quality=gps_fix_quality,
        )
    
    @staticmethod
    def _get_optional_float(data: dict, key: str) -> Optional[float]:
        """Safely extract optional float value from dictionary."""
        value = data.get(key)
        return float(value) if value is not None else None
    
    @staticmethod
    def _get_optional_int(data: dict, key: str) -> Optional[int]:
        """Safely extract optional integer value from dictionary."""
        value = data.get(key)
        return int(value) if value is not None else None
