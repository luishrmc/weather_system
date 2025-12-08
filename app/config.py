"""
Configuration management for the weather station application.
Loads settings from environment variables with sensible defaults.
"""
import os
from typing import List


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


class AppConfig:
    """Application-wide configuration."""
    
    def __init__(self):
        self.mqtt = MQTTConfig()
        self.log_level: str = os.getenv("LOG_LEVEL", "INFO")
        
        # Timezone for timestamps
        self.timezone: str = os.getenv("TZ", "America/Sao_Paulo")
    
    def __repr__(self) -> str:
        return f"AppConfig(mqtt={self.mqtt}, log_level='{self.log_level}')"


# Global config instance (loaded on import)
config = AppConfig()
