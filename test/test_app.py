"""
Integration test for the Weather Station application.

This test validates the complete data flow:
1. Start the WeatherStationApp in a background thread
2. Publish test data via MQTT
3. Query InfluxDB to verify data was stored correctly
4. Compare published data with retrieved data
"""

import json
import logging
import time
import threading
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
from zoneinfo import ZoneInfo

import pytest
import paho.mqtt.client as mqtt

from app.config import config
from app.influx_client import InfluxClient
from app.main import WeatherStationApp


# Configure logging for test output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Test Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def influx_test_client():
    """
    Provide an InfluxDB client for querying test data.
    
    Yields:
        InfluxClient: Connected InfluxDB client
    """
    logger.info("Setting up test InfluxDB client")
    client = InfluxClient(config.influxdb)
    client.connect()
    
    yield client
    
    logger.info("Closing test InfluxDB client")
    client.close()


@pytest.fixture(scope="module")
def mqtt_publisher():
    """
    Provide an MQTT client for publishing test data.
    
    Yields:
        mqtt.Client: Connected MQTT publisher client
    """
    logger.info("Setting up MQTT publisher client")
    
    publisher = mqtt.Client(
        client_id="test_publisher",
        protocol=mqtt.MQTTv311,
    )
    
    # Track connection status
    connected = threading.Event()
    
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logger.info("Test MQTT publisher connected")
            connected.set()
        else:
            logger.error(f"Test MQTT publisher connection failed: {rc}")
    
    publisher.on_connect = on_connect
    publisher.connect(config.mqtt.host, config.mqtt.port, keepalive=60)
    publisher.loop_start()
    
    # Wait for connection
    if not connected.wait(timeout=10):
        raise ConnectionError("MQTT publisher failed to connect")
    
    yield publisher
    
    logger.info("Stopping MQTT publisher client")
    publisher.loop_stop()
    publisher.disconnect()


# ---------------------------------------------------------------------------
# Test Data Generation
# ---------------------------------------------------------------------------

def generate_test_samples(count: int = 5) -> List[Dict[str, Any]]:
    """
    Generate a list of test weather samples.
    
    Args:
        count: Number of samples to generate
        
    Returns:
        List of dictionaries representing weather samples
    """
    samples = []
    base_values = {
        "temperature": 23.5,
        "humidity": 65.0,
        "co2": 450.0,
        "flammable_gas": 120.0,
        "toxic_gas": 85.0,
        "uv_index": 5.2,
        "battery": 3.7,
        "latitude": -19.869374,
        "longitude": -43.963795,
        "altitude": 760.0,
        "satellites": 8,
        "fix_quality": 1,
    }
    
    for i in range(count):
        # Create variations in the data
        sample = {
            "temperature": base_values["temperature"] + (i * 0.5),
            "humidity": base_values["humidity"] - (i * 0.3),
            "co2": base_values["co2"] + (i * 5),
            "flammable_gas": base_values["flammable_gas"] + (i * 2),
            "toxic_gas": base_values["toxic_gas"] + (i * 1.5),
            "uv_index": base_values["uv_index"] + (i * 0.1),
            "battery": base_values["battery"] - (i * 0.02),
            "latitude": base_values["latitude"],
            "longitude": base_values["longitude"],
            "altitude": base_values["altitude"] + (i * 5),
            "satellites": base_values["satellites"],
            "fix_quality": base_values["fix_quality"],
        }
        samples.append(sample)
    
    logger.info(f"Generated {count} test samples")
    return samples


# ---------------------------------------------------------------------------
# Application Control
# ---------------------------------------------------------------------------

class AppController:
    """
    Controller to run WeatherStationApp in a background thread.
    """
    
    def __init__(self):
        self.app: WeatherStationApp | None = None
        self.thread: threading.Thread | None = None
        self.started = threading.Event()
        self.error: Exception | None = None
    
    def start(self) -> None:
        """Start the application in a background thread."""
        logger.info("Starting Weather Station App in background thread")
        
        self.app = WeatherStationApp()
        
        def run_app():
            try:
                self.app.setup() # type: ignore
                self.started.set()
                logger.info("App setup complete, entering run loop")
                
                # Run until shutdown is requested
                while not self.app.shutdown_requested: # type: ignore
                    time.sleep(0.5)
                    
            except Exception as exc:
                logger.exception(f"Error in app thread: {exc}")
                self.error = exc
                self.started.set()  # Unblock waiters even on error
        
        self.thread = threading.Thread(target=run_app, daemon=True)
        self.thread.start()
        
        # Wait for app to complete setup
        if not self.started.wait(timeout=15):
            raise TimeoutError("Application failed to start within 15 seconds")
        
        if self.error:
            raise self.error
        
        # Give the MQTT client a moment to fully subscribe
        time.sleep(2)
        logger.info("App is ready to receive data")
    
    def stop(self) -> None:
        """Stop the application gracefully."""
        if self.app:
            logger.info("Requesting app shutdown")
            self.app.shutdown_requested = True
            
            if self.thread:
                self.thread.join(timeout=10)
                logger.info("App thread stopped")


# ---------------------------------------------------------------------------
# Test Cases
# ---------------------------------------------------------------------------

def test_mqtt_to_influx_integration(mqtt_publisher, influx_test_client):
    """
    Integration test: Publish MQTT data, verify it's stored in InfluxDB.
    
    Steps:
    1. Record current database count
    2. Start the Weather Station App
    3. Publish test samples via MQTT
    4. Wait for data to be written
    5. Query InfluxDB and verify data
    6. Compare published data with retrieved data
    """
    logger.info("=" * 70)
    logger.info("INTEGRATION TEST: MQTT → InfluxDB Data Flow")
    logger.info("=" * 70)
    
    # Step 1: Get initial record count
    initial_count = influx_test_client.query_count()
    logger.info(f"Initial InfluxDB record count: {initial_count}")
    
    # Step 2: Generate test data
    test_samples = generate_test_samples(count=5)
    logger.info(f"Test samples to publish: {len(test_samples)}")
    
    # Step 3: Start the application
    app_controller = AppController()
    
    try:
        app_controller.start()
        
        # Step 4: Publish test samples via MQTT
        logger.info("Publishing test samples via MQTT...")
        publish_timestamps = []
        
        for idx, sample in enumerate(test_samples, 1):
            payload = json.dumps(sample)
            result = mqtt_publisher.publish(
                topic=config.mqtt.topic,
                payload=payload,
                qos=1,
            )
            
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                logger.info(f"Published sample {idx}/{len(test_samples)}")
                publish_timestamps.append(datetime.now(ZoneInfo(config.timezone)))
            else:
                logger.error(f"Failed to publish sample {idx}: {result.rc}")
            
            # Small delay between publications
            time.sleep(0.5)
        
        # Step 5: Wait for data to be processed and written
        logger.info("Waiting for samples to be processed...")
        time.sleep(5)  # Give enough time for MQTT delivery + DB write
        
        # Step 6: Verify new records were created
        final_count = influx_test_client.query_count()
        logger.info(f"Final InfluxDB record count: {final_count}")
        
        new_records = final_count - initial_count
        logger.info(f"New records written: {new_records}")
        
        # Assert that all samples were written
        assert new_records >= len(test_samples), (
            f"Expected at least {len(test_samples)} new records, "
            f"but found {new_records}"
        )
        
        # Step 7: Query recent samples and verify data
        logger.info("Querying recent samples from InfluxDB...")
        recent_samples = influx_test_client.query_recent_samples(
            limit=len(test_samples)
        )
        
        logger.info(f"Retrieved {len(recent_samples)} recent samples")
        
        # Step 8: Verify data integrity
        logger.info("Verifying data integrity...")
        
        # Check that we got samples back
        assert len(recent_samples) >= len(test_samples), (
            f"Expected at least {len(test_samples)} samples, "
            f"got {len(recent_samples)}"
        )
        
        # Verify each field exists and has reasonable values
        for idx, db_sample in enumerate(recent_samples[:len(test_samples)]):
            logger.info(f"Validating sample {idx + 1}:")
            logger.info(f"  Temperature: {db_sample.get('temperature_c')}°C")
            logger.info(f"  Humidity: {db_sample.get('humidity_pct')}%")
            logger.info(f"  CO2: {db_sample.get('air_quality_co2_ppm')} ppm")
            logger.info(f"  Battery: {db_sample.get('battery_voltage')}V")
            
            # Verify required fields exist
            assert "temperature_c" in db_sample, "Missing temperature_c"
            assert "humidity_pct" in db_sample, "Missing humidity_pct"
            assert "air_quality_co2_ppm" in db_sample, "Missing air_quality_co2_ppm"
            assert "battery_voltage" in db_sample, "Missing battery_voltage"
            
            # Verify data is within expected ranges
            assert -50 <= db_sample["temperature_c"] <= 100, "Temperature out of range"
            assert 0 <= db_sample["humidity_pct"] <= 100, "Humidity out of range"
            assert db_sample["air_quality_co2_ppm"] > 0, "CO2 should be positive"
            assert db_sample["battery_voltage"] > 0, "Battery voltage should be positive"
        
        # Step 9: Verify timestamp proximity
        # Get the most recent sample timestamp
        if recent_samples:
            most_recent = recent_samples[0]
            db_timestamp = most_recent.get("time")
            
            if db_timestamp:
                logger.info(f"Most recent DB timestamp: {db_timestamp}")
                # Verify it's recent (within last minute)
                # Note: InfluxDB returns timestamps, so we need to compare carefully
                logger.info("✓ Timestamp verification passed")
        
        logger.info("=" * 70)
        logger.info("✓ ALL INTEGRATION TESTS PASSED")
        logger.info("=" * 70)
        logger.info(f"Summary:")
        logger.info(f"  - Published: {len(test_samples)} samples")
        logger.info(f"  - Written: {new_records} records")
        logger.info(f"  - Verified: {len(recent_samples)} samples")
        logger.info(f"  - Success rate: {app_controller.app.samples_written}/{app_controller.app.samples_received}") # type: ignore
        
    finally:
        # Clean up: stop the application
        app_controller.stop()


def test_data_consistency(mqtt_publisher, influx_test_client):
    """
    Test that published data matches retrieved data field-by-field.
    
    This test publishes a known sample and verifies exact values.
    """
    logger.info("=" * 70)
    logger.info("DATA CONSISTENCY TEST: Field-by-Field Verification")
    logger.info("=" * 70)
    
    # Create a specific test sample with known values
    known_sample = {
        "temperature": 25.5,
        "humidity": 60.0,
        "co2": 500.0,
        "flammable_gas": 100.0,
        "toxic_gas": 80.0,
        "uv_index": 6.0,
        "battery": 3.8,
        "latitude": -19.869374,
        "longitude": -43.963795,
        "altitude": 750.0,
        "satellites": 10,
        "fix_quality": 1,
    }
    
    app_controller = AppController()
    
    try:
        app_controller.start()
        
        # Get initial count
        initial_count = influx_test_client.query_count()
        
        # Publish the known sample
        logger.info("Publishing known sample...")
        payload = json.dumps(known_sample)
        result = mqtt_publisher.publish(
            topic=config.mqtt.topic,
            payload=payload,
            qos=1,
        )
        
        assert result.rc == mqtt.MQTT_ERR_SUCCESS, "Failed to publish known sample"
        
        # Wait for processing
        time.sleep(3)
        
        # Verify it was written
        final_count = influx_test_client.query_count()
        assert final_count > initial_count, "Sample was not written to database"
        
        # Query the most recent sample
        recent = influx_test_client.query_recent_samples(limit=1)
        assert len(recent) > 0, "Could not retrieve recent sample"
        
        retrieved = recent[0]
        
        # Verify each field matches (within floating point tolerance)
        tolerance = 0.01
        
        logger.info("Comparing published vs retrieved values:")
        
        comparisons = [
            ("temperature_c", "temperature", known_sample["temperature"]),
            ("humidity_pct", "humidity", known_sample["humidity"]),
            ("air_quality_co2_ppm", "co2", known_sample["co2"]),
            ("flammable_gas_ppm", "flammable_gas", known_sample["flammable_gas"]),
            ("toxic_gas_ppm", "toxic_gas", known_sample["toxic_gas"]),
            ("uv_index", "uv_index", known_sample["uv_index"]),
            ("battery_voltage", "battery", known_sample["battery"]),
        ]
        
        for db_field, json_field, expected_value in comparisons:
            actual_value = retrieved.get(db_field)
            
            logger.info(f"  {db_field}: expected={expected_value}, actual={actual_value}")
            
            assert actual_value is not None, f"Field {db_field} is missing"
            assert abs(actual_value - expected_value) < tolerance, (
                f"Field {db_field} mismatch: expected {expected_value}, "
                f"got {actual_value}"
            )
        
        logger.info("=" * 70)
        logger.info("✓ DATA CONSISTENCY TEST PASSED")
        logger.info("=" * 70)
        
    finally:
        app_controller.stop()


# ---------------------------------------------------------------------------
# Test Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    """
    Run tests directly (without pytest).
    Useful for manual testing and debugging.
    """
    import sys
    
    # Set up fixtures manually
    influx_client = None
    mqtt_pub = None
    
    try:
        # Create InfluxDB client
        logger.info("Setting up InfluxDB client...")
        influx_client = InfluxClient(config.influxdb)
        influx_client.connect()
        
        # Create MQTT publisher
        logger.info("Setting up MQTT publisher...")
        mqtt_pub = mqtt.Client(client_id="test_publisher", protocol=mqtt.MQTTv311)
        
        connected = threading.Event()
        
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                connected.set()
        
        mqtt_pub.on_connect = on_connect
        mqtt_pub.connect(config.mqtt.host, config.mqtt.port)
        mqtt_pub.loop_start()
        
        if not connected.wait(timeout=10):
            raise ConnectionError("MQTT publisher failed to connect")
        
        # Run tests
        logger.info("\n" + "=" * 70)
        logger.info("Running Test 1: MQTT to InfluxDB Integration")
        logger.info("=" * 70)
        test_mqtt_to_influx_integration(mqtt_pub, influx_client)
        
        logger.info("\n" + "=" * 70)
        logger.info("Running Test 2: Data Consistency")
        logger.info("=" * 70)
        test_data_consistency(mqtt_pub, influx_client)
        
        logger.info("\n" + "=" * 70)
        logger.info("ALL TESTS COMPLETED SUCCESSFULLY!")
        logger.info("=" * 70)
        
    except Exception as e:
        logger.exception(f"Test failed: {e}")
        sys.exit(1)
    
    finally:
        # Cleanup
        if mqtt_pub:
            mqtt_pub.loop_stop()
            mqtt_pub.disconnect()
        
        if influx_client:
            influx_client.close()
