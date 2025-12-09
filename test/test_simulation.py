"""
MQTT Weather Data Simulator

Continuously publishes realistic weather sensor data to MQTT for testing.
Configurable data ranges, sample rates, and variation patterns.

Usage:
    python test/test_simulation.py
    
    # With custom rate:
    python test/test_simulation.py --rate 2.0
    
    # With custom topic:
    python test/test_simulation.py --topic custom/topic
"""

import argparse
import json
import logging
import os
import random
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from math import sin, cos, pi
from typing import Optional

# Add parent directory to path to import app modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import paho.mqtt.client as mqtt

# Try to import config, but provide defaults if not available
try:
    from app.config import config as app_config
    DEFAULT_MQTT_HOST = app_config.mqtt.host
    DEFAULT_MQTT_PORT = app_config.mqtt.port
    DEFAULT_MQTT_TOPIC = app_config.mqtt.topic
except ImportError:
    DEFAULT_MQTT_HOST = "mosquitto"
    DEFAULT_MQTT_PORT = 1883
    DEFAULT_MQTT_TOPIC = "pse/weather_system/sensors"
    logging.warning("Could not import app.config, using defaults")


# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Sensor Configuration
# ---------------------------------------------------------------------------

@dataclass
class SensorConfig:
    """Configuration for a single sensor parameter."""
    min_value: float
    max_value: float
    baseline: float
    noise_amplitude: float = 0.0  # Random noise range
    trend_rate: float = 0.0  # How fast it trends up/down
    cycle_period: float = 0.0  # Period of cyclic variation (seconds)
    cycle_amplitude: float = 0.0  # Amplitude of cyclic variation


@dataclass
class SimulationConfig:
    """Complete simulation configuration."""
    
    # MQTT settings
    mqtt_host: str = DEFAULT_MQTT_HOST
    mqtt_port: int = DEFAULT_MQTT_PORT
    mqtt_topic: str = DEFAULT_MQTT_TOPIC
    
    # Simulation settings
    sample_rate: float = 1.0  # Samples per second (0.5 = every 2 seconds)
    
    # Sensor configurations
    temperature: SensorConfig = field(default_factory=lambda: SensorConfig(
        min_value=15.0,
        max_value=35.0,
        baseline=23.0,
        noise_amplitude=0.5,
        trend_rate=0.001,  # Slow drift
        cycle_period=3600.0,  # 1-hour cycle (simulating day/night)
        cycle_amplitude=5.0,
    ))
    
    humidity: SensorConfig = field(default_factory=lambda: SensorConfig(
        min_value=30.0,
        max_value=90.0,
        baseline=65.0,
        noise_amplitude=1.0,
        trend_rate=-0.0005,  # Inverse correlation with temp
        cycle_period=3600.0,
        cycle_amplitude=10.0,
    ))
    
    co2: SensorConfig = field(default_factory=lambda: SensorConfig(
        min_value=400.0,
        max_value=2000.0,
        baseline=450.0,
        noise_amplitude=20.0,
        trend_rate=0.005,
        cycle_period=1800.0,  # 30-minute cycle
        cycle_amplitude=100.0,
    ))
    
    flammable_gas: SensorConfig = field(default_factory=lambda: SensorConfig(
        min_value=50.0,
        max_value=500.0,
        baseline=120.0,
        noise_amplitude=10.0,
        trend_rate=0.0,
        cycle_period=0.0,  # No cycle, just noise
        cycle_amplitude=0.0,
    ))
    
    toxic_gas: SensorConfig = field(default_factory=lambda: SensorConfig(
        min_value=50.0,
        max_value=300.0,
        baseline=85.0,
        noise_amplitude=5.0,
        trend_rate=0.0,
        cycle_period=0.0,
        cycle_amplitude=0.0,
    ))
    
    uv_index: SensorConfig = field(default_factory=lambda: SensorConfig(
        min_value=0.0,
        max_value=11.0,
        baseline=5.0,
        noise_amplitude=0.3,
        trend_rate=0.0,
        cycle_period=7200.0,  # 2-hour cycle (sun position)
        cycle_amplitude=3.0,
    ))
    
    battery: SensorConfig = field(default_factory=lambda: SensorConfig(
        min_value=3.0,
        max_value=4.2,
        baseline=3.7,
        noise_amplitude=0.02,
        trend_rate=-0.00001,  # Slow discharge
        cycle_period=0.0,
        cycle_amplitude=0.0,
    ))
    
    # GPS settings
    gps_enabled: bool = True
    gps_latitude: float = -19.869494 
    gps_longitude: float = -43.964028
    gps_altitude: float = 760.0
    gps_noise: float = 0.00001  # Small GPS drift
    gps_satellites_min: int = 6
    gps_satellites_max: int = 12


# ---------------------------------------------------------------------------
# Sensor Simulator
# ---------------------------------------------------------------------------

class WeatherSimulator:
    """
    Simulates realistic weather sensor data with trends, cycles, and noise.
    """
    
    def __init__(self, config: SimulationConfig):
        """
        Initialize the simulator.
        
        Args:
            config: Simulation configuration
        """
        self.config = config
        self.start_time = time.time()
        self.sample_count = 0
        
        # Current sensor states (accumulated trends)
        self.current_trend = {
            "temperature": 0.0,
            "humidity": 0.0,
            "co2": 0.0,
            "flammable_gas": 0.0,
            "toxic_gas": 0.0,
            "uv_index": 0.0,
            "battery": 0.0,
        }
    
    def _apply_variation(
        self,
        sensor_config: SensorConfig,
        current_trend: float,
        elapsed_time: float,
    ) -> tuple[float, float]:
        """
        Apply realistic variations to a sensor value.
        
        Args:
            sensor_config: Configuration for this sensor
            current_trend: Current accumulated trend
            elapsed_time: Time since simulation start
            
        Returns:
            Tuple of (value, new_trend)
        """
        # Start with baseline
        value = sensor_config.baseline
        
        # Add trend (accumulated drift)
        current_trend += sensor_config.trend_rate
        value += current_trend
        
        # Add cyclic variation (simulating day/night, etc.)
        if sensor_config.cycle_period > 0:
            phase = (elapsed_time % sensor_config.cycle_period) / sensor_config.cycle_period
            cycle_value = sin(2 * pi * phase) * sensor_config.cycle_amplitude
            value += cycle_value
        
        # Add random noise
        if sensor_config.noise_amplitude > 0:
            noise = random.uniform(
                -sensor_config.noise_amplitude,
                sensor_config.noise_amplitude
            )
            value += noise
        
        # Clamp to valid range
        value = max(sensor_config.min_value, min(sensor_config.max_value, value))
        
        return value, current_trend
    
    def generate_sample(self) -> dict:
        """
        Generate a single weather sample with realistic variations.
        
        Returns:
            Dictionary representing a weather sample
        """
        elapsed_time = time.time() - self.start_time
        self.sample_count += 1
        
        # Generate each sensor value
        temperature, self.current_trend["temperature"] = self._apply_variation(
            self.config.temperature,
            self.current_trend["temperature"],
            elapsed_time,
        )
        
        humidity, self.current_trend["humidity"] = self._apply_variation(
            self.config.humidity,
            self.current_trend["humidity"],
            elapsed_time,
        )
        
        co2, self.current_trend["co2"] = self._apply_variation(
            self.config.co2,
            self.current_trend["co2"],
            elapsed_time,
        )
        
        flammable_gas, self.current_trend["flammable_gas"] = self._apply_variation(
            self.config.flammable_gas,
            self.current_trend["flammable_gas"],
            elapsed_time,
        )
        
        toxic_gas, self.current_trend["toxic_gas"] = self._apply_variation(
            self.config.toxic_gas,
            self.current_trend["toxic_gas"],
            elapsed_time,
        )
        
        uv_index, self.current_trend["uv_index"] = self._apply_variation(
            self.config.uv_index,
            self.current_trend["uv_index"],
            elapsed_time,
        )
        
        battery, self.current_trend["battery"] = self._apply_variation(
            self.config.battery,
            self.current_trend["battery"],
            elapsed_time,
        )
        
        # Generate GPS data (if enabled)
        gps_data = {}
        if self.config.gps_enabled:
            gps_data = {
                "latitude": self.config.gps_latitude + random.uniform(
                    -self.config.gps_noise, self.config.gps_noise
                ),
                "longitude": self.config.gps_longitude + random.uniform(
                    -self.config.gps_noise, self.config.gps_noise
                ),
                "altitude": self.config.gps_altitude + random.uniform(-2.0, 2.0),
                "satellites": random.randint(
                    self.config.gps_satellites_min,
                    self.config.gps_satellites_max,
                ),
                "fix_quality": 1,
            }
        
        # Construct the sample
        sample = {
            "temperature": round(temperature, 2),
            "humidity": round(humidity, 2),
            "co2": round(co2, 1),
            "flammable_gas": round(flammable_gas, 1),
            "toxic_gas": round(toxic_gas, 1),
            "uv_index": round(uv_index, 2),
            "battery": round(battery, 3),
            **gps_data,
        }
        
        return sample


# ---------------------------------------------------------------------------
# MQTT Publisher
# ---------------------------------------------------------------------------

class MQTTSimulator:
    """
    MQTT client that publishes simulated sensor data.
    """
    
    def __init__(self, sim_config: SimulationConfig):
        """
        Initialize the MQTT simulator.
        
        Args:
            sim_config: Simulation configuration
        """
        self.config = sim_config
        self.simulator = WeatherSimulator(sim_config)
        
        # MQTT client
        self.client = mqtt.Client(
            client_id="weather_simulator",
            protocol=mqtt.MQTTv311,
        )
        
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        
        self.connected = False
        self.running = False
        self.samples_published = 0
        self.publish_errors = 0
    
    def _on_connect(self, client, userdata, flags, rc):
        """Callback when connected to MQTT broker."""
        if rc == 0:
            logger.info("✓ Connected to MQTT broker at %s:%d",
                       self.config.mqtt_host, self.config.mqtt_port)
            self.connected = True
        else:
            logger.error("✗ MQTT connection failed with code %d", rc)
            self.connected = False
    
    def _on_disconnect(self, client, userdata, rc):
        """Callback when disconnected from MQTT broker."""
        self.connected = False
        if rc != 0:
            logger.warning("Unexpected disconnection from MQTT broker (code %d)", rc)
        else:
            logger.info("Disconnected from MQTT broker")
    
    def connect(self) -> None:
        """Connect to the MQTT broker."""
        logger.info("Connecting to MQTT broker at %s:%d...",
                   self.config.mqtt_host, self.config.mqtt_port)
        
        try:
            self.client.connect(
                self.config.mqtt_host,
                self.config.mqtt_port,
                keepalive=60,
            )
            self.client.loop_start()
            
            # Wait for connection
            timeout = 10
            start = time.time()
            while not self.connected and (time.time() - start) < timeout:
                time.sleep(0.1)
            
            if not self.connected:
                raise ConnectionError("Failed to connect to MQTT broker")
                
        except Exception as exc:
            logger.error("Connection error: %s", exc)
            raise
    
    def start(self) -> None:
        """Start publishing simulated data."""
        logger.info("=" * 70)
        logger.info("Weather Simulator Started")
        logger.info("=" * 70)
        logger.info("Configuration:")
        logger.info("  Topic: %s", self.config.mqtt_topic)
        logger.info("  Sample rate: %.2f samples/sec", self.config.sample_rate)
        logger.info("  Interval: %.2f seconds", 1.0 / self.config.sample_rate)
        logger.info("=" * 70)
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 70)
        
        self.running = True
        interval = 1.0 / self.config.sample_rate
        
        try:
            while self.running:
                start_time = time.time()
                
                # Generate sample
                sample = self.simulator.generate_sample()
                
                # Publish to MQTT
                try:
                    payload = json.dumps(sample)
                    result = self.client.publish(
                        self.config.mqtt_topic,
                        payload,
                        qos=1,
                    )
                    
                    if result.rc == mqtt.MQTT_ERR_SUCCESS:
                        self.samples_published += 1
                        
                        # Log every 10 samples
                        if self.samples_published % 10 == 0:
                            logger.info(
                                "[%d] Published | T: %.1f°C | H: %.1f%% | "
                                "CO2: %.0f ppm | Battery: %.2fV",
                                self.samples_published,
                                sample["temperature"],
                                sample["humidity"],
                                sample["co2"],
                                sample["battery"],
                            )
                    else:
                        self.publish_errors += 1
                        logger.error("Publish failed with code %d", result.rc)
                
                except Exception as exc:
                    self.publish_errors += 1
                    logger.error("Error publishing sample: %s", exc)
                
                # Sleep to maintain sample rate
                elapsed = time.time() - start_time
                sleep_time = max(0, interval - elapsed)
                time.sleep(sleep_time)
        
        except KeyboardInterrupt:
            logger.info("\nKeyboard interrupt received")
        
        finally:
            self.stop()
    
    def stop(self) -> None:
        """Stop the simulator and disconnect."""
        self.running = False
        
        logger.info("=" * 70)
        logger.info("Stopping Weather Simulator")
        logger.info("=" * 70)
        logger.info("Statistics:")
        logger.info("  Samples published: %d", self.samples_published)
        logger.info("  Publish errors: %d", self.publish_errors)
        logger.info("  Success rate: %.1f%%",
                   (self.samples_published / max(1, self.samples_published + self.publish_errors)) * 100)
        logger.info("=" * 70)
        
        self.client.loop_stop()
        self.client.disconnect()


# ---------------------------------------------------------------------------
# Main Entry Point
# ---------------------------------------------------------------------------

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="MQTT Weather Data Simulator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument(
        "--host",
        type=str,
        default=DEFAULT_MQTT_HOST,
        help=f"MQTT broker host (default: {DEFAULT_MQTT_HOST})",
    )
    
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_MQTT_PORT,
        help=f"MQTT broker port (default: {DEFAULT_MQTT_PORT})",
    )
    
    parser.add_argument(
        "--topic",
        type=str,
        default=DEFAULT_MQTT_TOPIC,
        help=f"MQTT topic (default: {DEFAULT_MQTT_TOPIC})",
    )
    
    parser.add_argument(
        "--rate",
        type=float,
        default=1.0,
        help="Sample rate in samples/second (default: 1.0)",
    )
    
    parser.add_argument(
        "--no-gps",
        action="store_true",
        help="Disable GPS data in samples",
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    
    # Create simulation configuration
    sim_config = SimulationConfig(
        mqtt_host=args.host,
        mqtt_port=args.port,
        mqtt_topic=args.topic,
        sample_rate=args.rate,
        gps_enabled=not args.no_gps,
    )
    
    # Create and start simulator
    simulator = MQTTSimulator(sim_config)
    
    try:
        simulator.connect()
        simulator.start()
    except Exception as exc:
        logger.exception("Simulator failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
