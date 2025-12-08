"""
Simple test script for MQTT client.
Run this to verify MQTT connection and JSON parsing.
"""
import logging
import time

from app.mqtt_client import MQTTClient
from app.config import config
from app.models import WeatherSample


# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def on_sample_received(sample: WeatherSample):
    """Callback invoked when a weather sample is received."""
    logger.info("=" * 60)
    logger.info("RECEIVED WEATHER SAMPLE:")
    logger.info(f"  Timestamp: {sample.timestamp}")
    logger.info(f"  Temperature: {sample.temperature_c}°C")
    logger.info(f"  Humidity: {sample.humidity_pct}%")
    logger.info(f"  CO2: {sample.air_quality_co2_ppm} ppm")
    logger.info(f"  Flammable Gas: {sample.flammable_gas_ppm} ppm")
    logger.info(f"  Toxic Gas: {sample.toxic_gas_ppm} ppm")
    logger.info(f"  UV Index: {sample.uv_index}")
    logger.info(f"  Battery: {sample.battery_voltage}V")
    
    if sample.gps_latitude and sample.gps_longitude:
        logger.info(f"  GPS: ({sample.gps_latitude}, {sample.gps_longitude})")
        logger.info(f"  Altitude: {sample.gps_altitude_m}m")
        logger.info(f"  Satellites: {sample.gps_satellites}")
    else:
        logger.info(f"  GPS: No fix")
    
    logger.info("=" * 60)


def main():
    """Main test function."""
    logger.info("Starting MQTT client test...")
    logger.info(f"Configuration: {config.mqtt}")
    
    # Create MQTT client
    mqtt_client = MQTTClient(
        config=config.mqtt,
        on_sample_received=on_sample_received,
    )
    
    try:
        # Connect and start
        mqtt_client.connect()
        mqtt_client.start()
        
        logger.info("Waiting for messages... (Press Ctrl+C to stop)")
        
        # Keep running
        while True:
            time.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    
    except Exception as e:
        logger.exception(f"Error: {e}")
    
    finally:
        mqtt_client.stop()
        logger.info("Test completed")


if __name__ == "__main__":
    main()
