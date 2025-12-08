"""
InfluxDB 3 Core client for storing and querying weather station data.
Provides a clean wrapper around the influxdb3-python client.
"""
import logging
from datetime import datetime, timedelta
from typing import List, Optional

from influxdb_client_3 import InfluxDBClient3, Point
from influxdb_client_3.write_client.client.write_api import WriteOptions

from app.models import WeatherSample
from app.config import InfluxDBConfig

logger = logging.getLogger(__name__)

class InfluxClient:
    """
    Thin wrapper around InfluxDB 3 Core client for weather data storage.
    Handles connection, point construction, and basic queries.
    """
    
    def __init__(self, config: InfluxDBConfig):
        """
        Initialize the InfluxDB client.
        
        Args:
            config: InfluxDB connection configuration
        """
        self.config = config
        self._client: Optional[InfluxDBClient3] = None
        
        logger.info(f"Initializing InfluxDB client for {config.url}")
    
    def connect(self) -> None:
        """
        Establish connection to InfluxDB 3 Core.
        
        Raises:
            ConnectionError: If connection fails
        """
        try:
            self._client = InfluxDBClient3(
                host=self.config.url,
                token=self.config.token,
                database=self.config.database,
                org=self.config.org,
            )
            
            logger.info(
                f"Connected to InfluxDB at {self.config.url} "
                f"(database: {self.config.database})"
            )
            
        except Exception as e:
            logger.error(f"Failed to connect to InfluxDB: {e}")
            raise ConnectionError(f"InfluxDB connection failed: {e}") from e
    
    def close(self) -> None:
        """Close the InfluxDB client connection."""
        if self._client:
            self._client.close()
            logger.info("InfluxDB connection closed")
    
    def is_connected(self) -> bool:
        """Check if client is connected."""
        return self._client is not None
    
    # -------------------------------------------------------------------------
    # Write Operations
    # -------------------------------------------------------------------------
    
    def write_sample(self, sample: WeatherSample) -> None:
        """
        Write a single WeatherSample to InfluxDB.
        
        Args:
            sample: The weather sample to write
            
        Raises:
            RuntimeError: If client is not connected
            Exception: If write operation fails
        """
        if not self._client:
            raise RuntimeError("InfluxDB client is not connected. Call connect() first.")
        
        try:
            # Convert WeatherSample to InfluxDB Point
            point = self._sample_to_point(sample)
            
            # Write to database
            self._client.write(record=point)
            
            logger.debug(f"Wrote sample to InfluxDB: {sample}")
            
        except Exception as e:
            logger.error(f"Failed to write sample to InfluxDB: {e}")
            raise
    
    def write_samples_batch(self, samples: List[WeatherSample]) -> None:
        """
        Write multiple WeatherSamples in a batch operation.
        More efficient than individual writes for bulk data.
        
        Args:
            samples: List of weather samples to write
            
        Raises:
            RuntimeError: If client is not connected
        """
        if not self._client:
            raise RuntimeError("InfluxDB client is not connected. Call connect() first.")
        
        if not samples:
            logger.warning("write_samples_batch called with empty list")
            return
        
        try:
            points = [self._sample_to_point(s) for s in samples]
            self._client.write(record=points)
            
            logger.info(f"Wrote {len(samples)} samples to InfluxDB")
            
        except Exception as e:
            logger.error(f"Failed to write batch to InfluxDB: {e}")
            raise
    
    def _sample_to_point(self, sample: WeatherSample) -> Point:
        """
        Convert a WeatherSample to an InfluxDB Point.
        
        Structure:
        - Measurement: weather_data (or configured name)
        - Tags: (optional, for now we use none, but could add location, device_id, etc.)
        - Fields: all sensor readings
        - Timestamp: server-side reception time
        
        Args:
            sample: The weather sample
            
        Returns:
            InfluxDB Point ready to write
        """
        point = Point(self.config.measurement)
        
        # Timestamp
        point.time(sample.timestamp)
        
        # Tags (optional - useful for filtering/grouping)
        # Example: point.tag("location", "outdoor")
        # Example: point.tag("device_id", "esp32_001")
        
        # Fields - all sensor data
        point.field("temperature_c", sample.temperature_c)
        point.field("humidity_pct", sample.humidity_pct)
        point.field("air_quality_co2_ppm", sample.air_quality_co2_ppm)
        point.field("flammable_gas_ppm", sample.flammable_gas_ppm)
        point.field("toxic_gas_ppm", sample.toxic_gas_ppm)
        point.field("uv_index", sample.uv_index)
        point.field("battery_voltage", sample.battery_voltage)
        
        # GPS fields (only if available)
        if sample.gps_latitude is not None:
            point.field("gps_latitude", sample.gps_latitude)
        
        if sample.gps_longitude is not None:
            point.field("gps_longitude", sample.gps_longitude)
        
        if sample.gps_altitude_m is not None:
            point.field("gps_altitude_m", sample.gps_altitude_m)
        
        if sample.gps_satellites is not None:
            point.field("gps_satellites", sample.gps_satellites)
        
        if sample.gps_fix_quality is not None:
            point.field("gps_fix_quality", sample.gps_fix_quality)
        
        return point
    
    # -------------------------------------------------------------------------
    # Query Operations
    # -------------------------------------------------------------------------
    
    def query_recent_samples(self, limit: int = 100) -> List[dict]:
        """
        Query the most recent weather samples.
        
        Args:
            limit: Maximum number of samples to return
            
        Returns:
            List of dictionaries containing sample data
            
        Raises:
            RuntimeError: If client is not connected
        """
        if not self._client:
            raise RuntimeError("InfluxDB client is not connected. Call connect() first.")
        
        query = f"""
            SELECT *
            FROM "{self.config.measurement}"
            ORDER BY time DESC
            LIMIT {limit}
        """
        
        try:
            result = self._client.query(query=query)
            
            # Convert PyArrow table to list of dicts
            data = self._pyarrow_to_list(result)
            
            logger.debug(f"Queried {len(data)} recent samples")
            return data
            
        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise
    
    def query_time_range(
        self,
        start: datetime,
        end: Optional[datetime] = None,
    ) -> List[dict]:
        """
        Query samples within a time range.
        
        Args:
            start: Start time (inclusive)
            end: End time (inclusive). If None, uses current time.
            
        Returns:
            List of dictionaries containing sample data
        """
        if not self._client:
            raise RuntimeError("InfluxDB client is not connected. Call connect() first.")
        
        if end is None:
            end = datetime.now(start.tzinfo)
        
        # Format timestamps for SQL query
        start_str = start.isoformat()
        end_str = end.isoformat()
        
        query = f"""
            SELECT *
            FROM "{self.config.measurement}"
            WHERE time >= '{start_str}' AND time <= '{end_str}'
            ORDER BY time ASC
        """
        
        try:
            result = self._client.query(query=query)
            
            # Convert PyArrow table to list of dicts
            data = self._pyarrow_to_list(result)
            
            logger.debug(f"Queried {len(data)} samples in time range")
            return data
            
        except Exception as e:
            logger.error(f"Time range query failed: {e}")
            raise
    
    def query_latest_sample(self) -> Optional[dict]:
        """
        Query the single most recent sample.
        
        Returns:
            Dictionary with the latest sample, or None if no data exists
        """
        results = self.query_recent_samples(limit=1)
        return results[0] if results else None
    
    def query_count(self) -> int:
        """
        Get the total count of records in the measurement.
        
        Returns:
            Total number of samples stored
        """
        if not self._client:
            raise RuntimeError("InfluxDB client is not connected. Call connect() first.")
        
        query = f"""
            SELECT COUNT(*) as count
            FROM "{self.config.measurement}"
        """
        
        try:
            result = self._client.query(query=query)
            
            # Convert PyArrow result to list of dicts
            data = self._pyarrow_to_list(result)
            
            if data and len(data) > 0:
                # COUNT(*) returns a single row with 'count' field
                return int(data[0].get('count', 0))
            
            return 0
            
        except Exception as e:
            logger.error(f"Count query failed: {e}")
            raise
    
    # -------------------------------------------------------------------------
    # Helper Methods
    # -------------------------------------------------------------------------
    
    @staticmethod
    def _pyarrow_to_list(table) -> List[dict]:
        """
        Convert PyArrow Table/RecordBatch to list of dictionaries.
        
        The influxdb3-python client returns PyArrow tables, which need
        to be converted to native Python types for easier consumption.
        
        Args:
            table: PyArrow Table or RecordBatch from query result
            
        Returns:
            List of dictionaries, one per row
        """
        try:
            # Convert to pandas DataFrame first (easiest conversion)
            df = table.to_pandas()
            
            # Convert DataFrame to list of dicts
            return df.to_dict('records')
            
        except Exception as e:
            logger.error(f"Failed to convert PyArrow table: {e}")
            
            # Fallback: manual conversion
            try:
                result = []
                
                # Get column names
                columns = table.column_names if hasattr(table, 'column_names') else table.schema.names
                
                # Iterate over rows
                for i in range(len(table)):
                    row_dict = {}
                    for col in columns:
                        value = table[col][i].as_py()  # Convert to Python object
                        row_dict[col] = value
                    result.append(row_dict)
                
                return result
                
            except Exception as e2:
                logger.error(f"Fallback conversion also failed: {e2}")
                raise
