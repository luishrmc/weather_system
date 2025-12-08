from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class WeatherSample:
    """
    Represents a complete weather station reading.
    Timestamp is assigned by the container (Server Time).
    """
    timestamp: datetime
    
    # Sensors
    temperature_c: float        # DHT22
    humidity_pct: float         # DHT22
    air_quality_co2_ppm: float  # SCD40
    flammable_gas_ppm: float    # MQ-9
    toxic_gas_ppm: float        # MQ-135
    uv_index: float             # GUVA-S12SD
    battery_voltage: float
    
    # GPS (LC76G)
    gps_latitude: Optional[float] = None
    gps_longitude: Optional[float] = None
    gps_altitude_m: Optional[float] = None
    gps_satellites: Optional[int] = None
    gps_fix_quality: Optional[int] = None
    
    def __post_init__(self):
        """Validate data ranges to prevent bad data from polluting the DB."""
        if not isinstance(self.timestamp, datetime):
             raise TypeError(f"timestamp must be datetime, got {type(self.timestamp)}")

        # Basic Sanity Checks (expand as needed)
        if not (-50 <= self.temperature_c <= 100):
            raise ValueError(f"Temp out of range: {self.temperature_c}")
        
        if not (0 <= self.humidity_pct <= 100):
            raise ValueError(f"Humidity out of range: {self.humidity_pct}")

    def __repr__(self):
        return (f"[{self.timestamp.strftime('%H:%M:%S')}] "
                f"T:{self.temperature_c:.1f}C H:{self.humidity_pct:.1f}% "
                f"CO2:{self.air_quality_co2_ppm:.0f} "
                f"GPS:{self.gps_latitude},{self.gps_longitude}")