"""
Weather Station Application - Main Entry Point

Orchestrates the data flow: MQTT → InfluxDB 3 Core.
Subscribes to weather sensor data over MQTT and stores it in InfluxDB.
"""

import logging
import signal
import sys
import time
from typing import Optional

from app.config import config
from app.influx_client import InfluxClient
from app.models import WeatherSample
from app.mqtt_client import MQTTClient


# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=getattr(logging, config.log_level.upper(), logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        # Optionally add file handler:
        # logging.FileHandler("weather_station.log"),
    ],
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Main Application
# ---------------------------------------------------------------------------

class WeatherStationApp:
    """
    Main application class for the weather station.

    Responsibilities:
    - Initialize and manage MQTT and InfluxDB clients.
    - Receive WeatherSample objects via MQTT callback.
    - Persist samples to InfluxDB.
    - Track and report basic statistics.
    """

    def __init__(self) -> None:
        """Initialize the application but do not connect yet."""
        logger.info("=" * 70)
        logger.info("Weather Station Application Starting")
        logger.info("=" * 70)

        # Display effective configuration
        logger.info("Configuration: %s", config)

        # Statistics tracking
        self.samples_received: int = 0
        self.samples_written: int = 0
        self.samples_failed: int = 0
        self.start_time: float = time.time()

        # Clients (created in setup)
        self.influx_client: Optional[InfluxClient] = None
        self.mqtt_client: Optional[MQTTClient] = None

        # Shutdown flag
        self.shutdown_requested: bool = False

    # ------------------------------------------------------------------ #
    # Setup & lifecycle
    # ------------------------------------------------------------------ #

    def setup(self) -> None:
        """
        Set up connections to InfluxDB and MQTT broker.

        Raises:
            ConnectionError: If either connection fails.
        """
        logger.info("Setting up connections...")

        # 1. Connect to InfluxDB first
        logger.info("Connecting to InfluxDB...")
        self.influx_client = InfluxClient(config.influxdb)
        self.influx_client.connect()

        # Optional: verify database accessibility
        try:
            count = self.influx_client.query_count()
            logger.info("InfluxDB ready. Current record count: %s", count)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not query initial count from InfluxDB: %s", exc)

        # 2. Set up MQTT client with callback
        logger.info("Setting up MQTT client...")
        self.mqtt_client = MQTTClient(
            config=config.mqtt,
            on_sample_received=self._on_sample_received,
        )

        # 3. Connect to MQTT broker and start loop
        self.mqtt_client.connect()
        self.mqtt_client.start()

        logger.info("=" * 70)
        logger.info("Setup complete! Waiting for sensor data...")
        logger.info("Subscribed to topic: %s", config.mqtt.topic)
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 70)

    # ------------------------------------------------------------------ #
    # MQTT callback and stats
    # ------------------------------------------------------------------ #

    def _on_sample_received(self, sample: WeatherSample) -> None:
        """
        Callback invoked when a WeatherSample is received from MQTT.

        Writes the sample to InfluxDB and updates statistics.

        Args:
            sample: Parsed and validated weather sample.
        """
        self.samples_received += 1

        if not self.influx_client:
            # Defensive guard: should never happen if setup() succeeded.
            self.samples_failed += 1
            logger.error("Influx client not initialized; cannot write sample.")
            logger.debug("Orphan sample: %s", sample)
            return

        try:
            self.influx_client.write_sample(sample)
            self.samples_written += 1

            # Log a concise summary of the sample
            logger.info(
                "[%d] %s | T: %.1f°C | H: %.1f%% | CO2: %.0f ppm | Battery: %.2fV",
                self.samples_received,
                sample.timestamp.strftime("%H:%M:%S"),
                sample.temperature_c,
                sample.humidity_pct,
                sample.air_quality_co2_ppm,
                sample.battery_voltage,
            )

            # Periodically show aggregated statistics
            if self.samples_received % 10 == 0:
                self._show_statistics()

        except Exception as exc:  # noqa: BLE001
            self.samples_failed += 1
            logger.error("Failed to write sample to InfluxDB: %s", exc)
            logger.debug("Failed sample: %s", sample)

    def _show_statistics(self) -> None:
        """Display application statistics in the logs."""
        uptime = time.time() - self.start_time
        uptime_str = time.strftime("%H:%M:%S", time.gmtime(uptime))

        success_rate = (
            (self.samples_written / self.samples_received * 100.0)
            if self.samples_received > 0
            else 0.0
        )

        logger.info("-" * 70)
        logger.info("Statistics | Uptime: %s", uptime_str)
        logger.info(
            "  Received: %d | Written: %d | Failed: %d | Success: %.1f%%",
            self.samples_received,
            self.samples_written,
            self.samples_failed,
            success_rate,
        )
        logger.info("-" * 70)

    # ------------------------------------------------------------------ #
    # Main loop and shutdown
    # ------------------------------------------------------------------ #

    def run(self) -> None:
        """
        Main application loop.

        Keeps the application running until shutdown is requested.
        """
        try:
            while not self.shutdown_requested:
                time.sleep(1.0)
                # Optional: periodic health checks could be added here.
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
            self.shutdown_requested = True
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        """Gracefully shut down all connections and display final statistics."""
        if not self.shutdown_requested:
            # If called directly without flag, mark shutdown
            self.shutdown_requested = True

        logger.info("=" * 70)
        logger.info("Shutting down Weather Station Application")
        logger.info("=" * 70)

        # Show final statistics
        self._show_statistics()

        # Stop MQTT client
        if self.mqtt_client is not None:
            logger.info("Stopping MQTT client...")
            try:
                self.mqtt_client.stop()
            except Exception:  # noqa: BLE001
                logger.exception("Error while stopping MQTT client")

        # Close InfluxDB connection
        if self.influx_client is not None:
            logger.info("Closing InfluxDB connection...")
            try:
                self.influx_client.close()
            except Exception:  # noqa: BLE001
                logger.exception("Error while closing InfluxDB client")

        logger.info("Shutdown complete. Goodbye!")
        logger.info("=" * 70)


# ---------------------------------------------------------------------------
# Signal handling and entrypoint
# ---------------------------------------------------------------------------

# Module-level reference for signal handler access
_APP_INSTANCE: Optional[WeatherStationApp] = None


def signal_handler(signum: int, frame: object | None) -> None:
    """
    Handle termination signals (SIGINT, SIGTERM).

    Triggers graceful shutdown of the application.
    
    Args:
        signum: Signal number received
        frame: Current stack frame (unused)
    """
    logger.info("Received signal %s", signum)
    global _APP_INSTANCE
    if _APP_INSTANCE is not None:
        _APP_INSTANCE.shutdown_requested = True
    else:
        # If app is not initialized yet, exit immediately
        sys.exit(0)


def main() -> None:
    """Main entry point for the application."""
    global _APP_INSTANCE

    app = WeatherStationApp()
    _APP_INSTANCE = app

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Set up connections
        app.setup()
        # Run main loop
        app.run()

    except ConnectionError as exc:
        logger.error("Connection failed: %s", exc)
        logger.error("Make sure MQTT broker and InfluxDB are running")
        sys.exit(1)

    except Exception as exc:  # noqa: BLE001
        logger.exception("Unexpected error: %s", exc)
        sys.exit(1)

    finally:
        # Ensure cleanup happens even if setup or run fails
        if _APP_INSTANCE is not None:
            _APP_INSTANCE.shutdown()


if __name__ == "__main__":
    main()
