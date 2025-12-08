"""
Configuration management for the weather station application.
Loads settings from environment variables with sensible defaults.
"""
import os
import json
import logging
from typing import Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class MQTTConfig:
    """MQTT broker connection configuration."""
    
    def __init__(self):
        self.host: str = os.getenv("MQTT_HOST", "mosquitto")
        self.port: int = int(os.getenv("MQTT_PORT", "1883"))
        self.topic: str = os.getenv("MQTT_TOPIC", "pse/weather_system/sensors")
        
        # Authentication (optional, currently not used)
        self.username: str | None = os.getenv("MQTT_USERNAME")
        self.password: str | None = os.getenv("MQTT_PASSWORD")
        
        # Connection settings
        self.keepalive: int = int(os.getenv("MQTT_KEEPALIVE", "60"))
        self.client_id: str = os.getenv("MQTT_CLIENT_ID", "weather_station_python")
        
        # QoS levels
        self.qos: int = int(os.getenv("MQTT_QOS", "1"))  # 0=at most once, 1=at least once, 2=exactly once
    
    def __repr__(self) -> str:
        """String representation (hides password)."""
        return (
            f"MQTTConfig(host='{self.host}', port={self.port}, "
            f"topic='{self.topic}', client_id='{self.client_id}')"
        )


class InfluxDBConfig:
    """InfluxDB 3 Core connection configuration."""
    
    def __init__(self):
        self.host: str = os.getenv("INFLUX_HOST", "influxdb3-core-pse")
        self.port: int = int(os.getenv("INFLUX_PORT", "8181"))
        
        # Database and organization
        self.database: str = os.getenv("INFLUX_DATABASE", "weather_station")
        self.org: str = os.getenv("INFLUX_ORG", "pse")
        
        # Measurement (table) name
        self.measurement: str = os.getenv("INFLUX_MEASUREMENT", "weather_data")
        
        # Token handling: try env var first, then fall back to token.json
        self.token: str = self._load_token()
        
        # Connection URL
        self.url: str = f"http://{self.host}:{self.port}"
    
    def _load_token(self) -> str:
        """
        Load InfluxDB token from environment variable or token.json file.
        
        Returns:
            The authentication token
            
        Raises:
            ValueError: If token cannot be found
        """
        # Try environment variable first
        token = os.getenv("INFLUX_TOKEN")
        if token:
            logger.info("Using InfluxDB token from environment variable")
            return token
        
        # Fall back to token.json file
        token_file = Path("config/influxdb3/token.json")
        
        if token_file.exists():
            try:
                with open(token_file, 'r') as f:
                    token_data = json.load(f)
                    token = token_data.get("token")
                    
                    if token:
                        logger.info(f"Loaded InfluxDB token from {token_file}")
                        return token
                    else:
                        raise ValueError("Token field not found in token.json")
                        
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in {token_file}: {e}")
            except Exception as e:
                raise ValueError(f"Failed to read token file: {e}")
        
        # No token found
        raise ValueError(
            "InfluxDB token not found. Set INFLUX_TOKEN env var or "
            "create config/influxdb3/token.json with token field"
        )
    
    def __repr__(self) -> str:
        """String representation (hides token)."""
        token_preview = f"{self.token[:10]}..." if self.token else "None"
        return (
            f"InfluxDBConfig(url='{self.url}', database='{self.database}', "
            f"org='{self.org}', measurement='{self.measurement}', "
            f"token='{token_preview}')"
        )


class AppConfig:
    """Application-wide configuration."""
    
    def __init__(self):
        self.mqtt = MQTTConfig()
        self.influxdb = InfluxDBConfig()
        self.log_level: str = os.getenv("LOG_LEVEL", "INFO")
        
        # Timezone for timestamps
        self.timezone: str = os.getenv("TZ", "America/Sao_Paulo")
    
    def __repr__(self) -> str:
        return (
            f"AppConfig(mqtt={self.mqtt}, influxdb={self.influxdb}, "
            f"log_level='{self.log_level}')"
        )


# Global config instance (loaded on import)
config = AppConfig()
