"""
Test script for InfluxDB client.
Tests connection, write operations, and queries.
"""
import logging
import time
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from app.influx_client import InfluxClient
from app.config import config
from app.models import WeatherSample

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def create_dummy_sample(offset_seconds: int = 0) -> WeatherSample:
    """Create a dummy weather sample for testing."""
    tz = ZoneInfo(config.timezone)
    timestamp = datetime.now(tz) - timedelta(seconds=offset_seconds)
    
    return WeatherSample(
        timestamp=timestamp,
        temperature_c=23.5 + (offset_seconds * 0.1),
        humidity_pct=65.2 - (offset_seconds * 0.05),
        air_quality_co2_ppm=450.0 + (offset_seconds * 2),
        flammable_gas_ppm=120.5,
        toxic_gas_ppm=85.3,
        uv_index=5.2,
        battery_voltage=3.7 - (offset_seconds * 0.001),
        gps_latitude=-23.550520,
        gps_longitude=-46.633308,
        gps_altitude_m=760.0,
        gps_satellites=8,
        gps_fix_quality=1,
    )


def test_connection():
    """Test 1: Connection to InfluxDB."""
    logger.info("=" * 60)
    logger.info("TEST 1: Connection")
    logger.info("=" * 60)
    
    influx = InfluxClient(config.influxdb)
    
    try:
        influx.connect()
        logger.info("✓ Connection successful")
        return influx
    except Exception as e:
        logger.error(f"✗ Connection failed: {e}")
        raise


def test_write_single(influx: InfluxClient):
    """Test 2: Write a single sample."""
    logger.info("=" * 60)
    logger.info("TEST 2: Write Single Sample")
    logger.info("=" * 60)
    
    sample = create_dummy_sample()
    
    try:
        influx.write_sample(sample)
        logger.info(f"✓ Wrote sample: {sample}")
    except Exception as e:
        logger.error(f"✗ Write failed: {e}")
        raise


def test_write_batch(influx: InfluxClient):
    """Test 3: Write multiple samples in batch."""
    logger.info("=" * 60)
    logger.info("TEST 3: Write Batch Samples")
    logger.info("=" * 60)
    
    # Create 5 samples with different timestamps
    samples = [create_dummy_sample(offset_seconds=i * 10) for i in range(5)]
    
    try:
        influx.write_samples_batch(samples)
        logger.info(f"✓ Wrote {len(samples)} samples in batch")
    except Exception as e:
        logger.error(f"✗ Batch write failed: {e}")
        raise


def test_query_count(influx: InfluxClient):
    """Test 4: Count total records."""
    logger.info("=" * 60)
    logger.info("TEST 4: Query Record Count")
    logger.info("=" * 60)
    
    try:
        count = influx.query_count()
        logger.info(f"✓ Total records in database: {count}")
    except Exception as e:
        logger.error(f"✗ Count query failed: {e}")
        raise


def test_query_latest(influx: InfluxClient):
    """Test 5: Query latest sample."""
    logger.info("=" * 60)
    logger.info("TEST 5: Query Latest Sample")
    logger.info("=" * 60)
    
    try:
        latest = influx.query_latest_sample()
        
        if latest:
            logger.info("✓ Latest sample:")
            logger.info(f"  Time: {latest.get('time')}")
            logger.info(f"  Temperature: {latest.get('temperature_c')}°C")
            logger.info(f"  Humidity: {latest.get('humidity_pct')}%")
            logger.info(f"  CO2: {latest.get('air_quality_co2_ppm')} ppm")
        else:
            logger.warning("✓ No data in database yet")
            
    except Exception as e:
        logger.error(f"✗ Latest query failed: {e}")
        raise


def test_query_recent(influx: InfluxClient):
    """Test 6: Query recent samples."""
    logger.info("=" * 60)
    logger.info("TEST 6: Query Recent Samples")
    logger.info("=" * 60)
    
    try:
        samples = influx.query_recent_samples(limit=10)
        
        logger.info(f"✓ Retrieved {len(samples)} recent samples:")
        for i, sample in enumerate(samples[:3], 1):  # Show first 3
            logger.info(f"  {i}. Time: {sample.get('time')}, "
                       f"Temp: {sample.get('temperature_c')}°C")
        
        if len(samples) > 3:
            logger.info(f"  ... and {len(samples) - 3} more")
            
    except Exception as e:
        logger.error(f"✗ Recent query failed: {e}")
        raise


def test_query_time_range(influx: InfluxClient):
    """Test 7: Query time range."""
    logger.info("=" * 60)
    logger.info("TEST 7: Query Time Range")
    logger.info("=" * 60)
    
    tz = ZoneInfo(config.timezone)
    end = datetime.now(tz)
    start = end - timedelta(hours=1)  # Last hour
    
    try:
        samples = influx.query_time_range(start=start, end=end)
        
        logger.info(f"✓ Retrieved {len(samples)} samples from last hour")
        logger.info(f"  Start: {start.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"  End: {end.strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        logger.error(f"✗ Time range query failed: {e}")
        raise


def main():
    """Run all tests."""
    logger.info("Starting InfluxDB client tests...")
    logger.info(f"Configuration: {config.influxdb}")
    
    influx = None
    
    try:
        # Test 1: Connection
        influx = test_connection()
        time.sleep(1)
        
        # Test 2: Write single sample
        test_write_single(influx)
        time.sleep(1)
        
        # Test 3: Write batch
        test_write_batch(influx)
        time.sleep(1)
        
        # Test 4: Count
        test_query_count(influx)
        time.sleep(1)
        
        # Test 5: Latest
        test_query_latest(influx)
        time.sleep(1)
        
        # Test 6: Recent
        test_query_recent(influx)
        time.sleep(1)
        
        # Test 7: Time range
        test_query_time_range(influx)
        
        logger.info("=" * 60)
        logger.info("ALL TESTS PASSED! ✓")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.exception(f"Test suite failed: {e}")
        
    finally:
        if influx:
            influx.close()
            logger.info("Closed InfluxDB connection")


if __name__ == "__main__":
    main()
