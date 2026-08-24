# tests/test_consumers.py
"""
Tests for Kafka consumers using mocked connections.
No live Kafka or PostGIS connections required.
"""

from unittest.mock import MagicMock, patch


class TestVesselConsumer:
    """Tests for vessel_consumer functions."""

    def test_upsert_vessel_executes_insert(self):
        """upsert_vessel executes INSERT with correct vessel data."""
        from ingestion.consumers.vessel_consumer import upsert_vessel

        cursor = MagicMock()
        vessel = {
            "mmsi": 563056100,
            "vessel_name": "PEARL MELODY",
            "latitude": 1.23738,
            "longitude": 103.75036,
            "speed_knots": 0.0,
            "heading": 111,
            "course": 152.1,
            "nav_status": 5,
            "source": "aisstream",
            "timestamp": "2026-04-08T18:25:04+00:00",
        }

        upsert_vessel(cursor, vessel)
        assert cursor.execute.called

    def test_upsert_vessel_skips_missing_coords(self):
        """upsert_vessel skips vessel with no coordinates."""
        from ingestion.consumers.vessel_consumer import upsert_vessel

        cursor = MagicMock()
        vessel = {
            "mmsi": 123456789,
            "vessel_name": "NO COORDS",
            "latitude": None,
            "longitude": None,
            "speed_knots": 0.0,
            "heading": 0,
            "course": 0.0,
            "nav_status": 0,
            "source": "aisstream",
        }

        inserted = upsert_vessel(cursor, vessel)
        assert not cursor.execute.called
        assert inserted is False

    def test_upsert_vessel_accepts_zero_coords(self):
        """upsert_vessel preserves positions on both zero axes."""
        from ingestion.consumers.vessel_consumer import upsert_vessel

        cursor = MagicMock()
        vessel = {
            "mmsi": 123456789,
            "vessel_name": "ZERO AXES",
            "latitude": 0.0,
            "longitude": 0.0,
            "speed_knots": 5.0,
            "heading": 90,
            "course": 90.0,
            "nav_status": 0,
            "source": "aisstream",
        }

        upsert_vessel(cursor, vessel)

        cursor.execute.assert_called_once()

    def test_upsert_vessel_adds_received_at(self):
        """upsert_vessel adds received_at timestamp to vessel dict."""
        from ingestion.consumers.vessel_consumer import upsert_vessel

        cursor = MagicMock()
        vessel = {
            "mmsi": 563056100,
            "vessel_name": "PEARL MELODY",
            "latitude": 1.23738,
            "longitude": 103.75036,
            "speed_knots": 0.0,
            "heading": 111,
            "course": 152.1,
            "nav_status": 5,
            "source": "aisstream",
        }

        upsert_vessel(cursor, vessel)
        assert "received_at" in vessel

    def test_build_consumer_uses_correct_topic(self):
        """build_consumer subscribes to orbitharbor.vessels topic."""
        with patch("ingestion.consumers.vessel_consumer.KafkaConsumer") as mock_kafka:
            from ingestion.consumers.vessel_consumer import build_consumer

            build_consumer()
            call_args = mock_kafka.call_args[0]
            assert "orbitharbor.vessels" in call_args

    def test_build_connection_uses_config(self):
        """build_connection uses postgis config values."""
        with patch("ingestion.consumers.vessel_consumer.psycopg2.connect") as mock_conn:
            from ingestion.consumers.vessel_consumer import build_connection

            build_connection()
            assert mock_conn.called


class TestAircraftConsumer:
    """Tests for aircraft_consumer functions."""

    def test_insert_aircraft_executes_insert(self):
        """insert_aircraft executes INSERT with correct aircraft data."""
        from ingestion.consumers.aircraft_consumer import insert_aircraft

        cursor = MagicMock()
        aircraft = {
            "icao24": "8a02ca",
            "callsign": "AWQ504",
            "origin_country": "Indonesia",
            "latitude": 1.1998,
            "longitude": 103.915,
            "altitude_m": 777.24,
            "velocity_ms": 93.83,
            "heading": 202.91,
            "vertical_rate": 8.13,
            "on_ground": False,
            "squawk": None,
            "source": "opensky",
        }

        insert_aircraft(cursor, aircraft)
        assert cursor.execute.called

    def test_insert_aircraft_skips_missing_coords(self):
        """insert_aircraft skips aircraft with no coordinates."""
        from ingestion.consumers.aircraft_consumer import insert_aircraft

        cursor = MagicMock()
        aircraft = {
            "icao24": "abc123",
            "callsign": "TEST",
            "origin_country": "Singapore",
            "latitude": None,
            "longitude": None,
            "altitude_m": None,
            "velocity_ms": None,
            "heading": None,
            "vertical_rate": None,
            "on_ground": False,
            "squawk": None,
            "source": "opensky",
        }

        insert_aircraft(cursor, aircraft)
        assert not cursor.execute.called

    def test_insert_aircraft_skips_out_of_range_coords(self):
        """insert_aircraft rejects coordinates outside WGS84 bounds."""
        from ingestion.consumers.aircraft_consumer import insert_aircraft

        cursor = MagicMock()
        aircraft = {
            "icao24": "abc123",
            "callsign": "INVALID",
            "origin_country": "Unknown",
            "latitude": -91.0,
            "longitude": 0.0,
            "altitude_m": 1000.0,
            "velocity_ms": 50.0,
            "heading": 0.0,
            "vertical_rate": 0.0,
            "on_ground": False,
            "squawk": None,
            "source": "opensky",
        }

        insert_aircraft(cursor, aircraft)

        cursor.execute.assert_not_called()

    def test_insert_aircraft_adds_received_at(self):
        """insert_aircraft adds received_at timestamp."""
        from ingestion.consumers.aircraft_consumer import insert_aircraft

        cursor = MagicMock()
        aircraft = {
            "icao24": "8a02ca",
            "callsign": "AWQ504",
            "origin_country": "Indonesia",
            "latitude": 1.1998,
            "longitude": 103.915,
            "altitude_m": 777.24,
            "velocity_ms": 93.83,
            "heading": 202.91,
            "vertical_rate": 8.13,
            "on_ground": False,
            "squawk": None,
            "source": "opensky",
        }

        insert_aircraft(cursor, aircraft)
        assert "received_at" in aircraft

    def test_build_consumer_uses_correct_topic(self):
        """build_consumer subscribes to orbitharbor.aircraft topic."""
        with patch("ingestion.consumers.aircraft_consumer.KafkaConsumer") as mock_kafka:
            from ingestion.consumers.aircraft_consumer import build_consumer

            build_consumer()
            call_args = mock_kafka.call_args[0]
            assert "orbitharbor.aircraft" in call_args


class TestLagMonitor:
    """Tests for lag_monitor functions."""

    def test_get_lag_returns_dict(self):
        """get_lag returns dict with partition lag data."""
        with patch("ingestion.consumers.lag_monitor.KafkaConsumer") as mock_kafka:
            mock_consumer = MagicMock()
            mock_kafka.return_value = mock_consumer

            from kafka import TopicPartition

            tp = TopicPartition("orbitharbor.vessels", 0)

            mock_consumer.partitions_for_topic.return_value = {0}
            mock_consumer.end_offsets.return_value = {tp: 100}
            mock_consumer.committed.return_value = 90

            from ingestion.consumers.lag_monitor import get_lag

            result = get_lag("localhost:9092", "vessel-consumer-group", "orbitharbor.vessels")

            assert isinstance(result, dict)
            assert 0 in result

    def test_get_lag_calculates_correctly(self):
        """get_lag correctly calculates lag as end - committed."""
        with patch("ingestion.consumers.lag_monitor.KafkaConsumer") as mock_kafka:
            mock_consumer = MagicMock()
            mock_kafka.return_value = mock_consumer

            from kafka import TopicPartition

            tp = TopicPartition("orbitharbor.vessels", 0)

            mock_consumer.partitions_for_topic.return_value = {0}
            mock_consumer.end_offsets.return_value = {tp: 100}
            mock_consumer.committed.return_value = 80

            from ingestion.consumers.lag_monitor import get_lag

            result = get_lag("localhost:9092", "vessel-consumer-group", "orbitharbor.vessels")

            assert result[0]["lag"] == 20
            assert result[0]["end_offset"] == 100
            assert result[0]["committed_offset"] == 80

    def test_get_lag_returns_empty_for_no_partitions(self):
        """get_lag returns empty dict when topic has no partitions."""
        with patch("ingestion.consumers.lag_monitor.KafkaConsumer") as mock_kafka:
            mock_consumer = MagicMock()
            mock_kafka.return_value = mock_consumer
            mock_consumer.partitions_for_topic.return_value = None

            from ingestion.consumers.lag_monitor import get_lag

            result = get_lag("localhost:9092", "vessel-consumer-group", "orbitharbor.vessels")

            assert result == {}


class TestVesselConsumerMain:
    """Tests for vessel_consumer main function."""

    def test_main_runs_and_exits_on_keyboard_interrupt(self):
        """main exits cleanly on KeyboardInterrupt."""
        with patch(
            "ingestion.consumers.vessel_consumer.build_connection"
        ) as mock_conn, patch(
            "ingestion.consumers.vessel_consumer.KafkaConsumer"
        ) as mock_kafka:

            mock_consumer = MagicMock()
            mock_consumer.__iter__ = MagicMock(side_effect=KeyboardInterrupt)
            mock_kafka.return_value = mock_consumer

            mock_cursor = MagicMock()
            mock_conn.return_value.cursor.return_value = mock_cursor

            from ingestion.consumers.vessel_consumer import main

            main()

            mock_cursor.close.assert_called()

    def test_main_avoids_commit_for_invalid_coordinates(self):
        """Invalid vessel messages do not create empty transactions."""
        message = MagicMock(
            value={"mmsi": 123456789, "latitude": None, "longitude": None}
        )

        with patch(
            "ingestion.consumers.vessel_consumer.build_connection"
        ) as mock_conn, patch(
            "ingestion.consumers.vessel_consumer.KafkaConsumer"
        ) as mock_kafka:
            mock_kafka.return_value.__iter__ = MagicMock(
                return_value=iter([message])
            )
            mock_cursor = MagicMock()
            mock_conn.return_value.cursor.return_value = mock_cursor

            from ingestion.consumers.vessel_consumer import main

            main()

            mock_cursor.execute.assert_not_called()
            mock_conn.return_value.commit.assert_not_called()


class TestAircraftConsumerMain:
    """Tests for aircraft_consumer main function."""

    def test_main_runs_and_exits_on_keyboard_interrupt(self):
        """main exits cleanly on KeyboardInterrupt."""
        with patch(
            "ingestion.consumers.aircraft_consumer.build_connection"
        ) as mock_conn, patch(
            "ingestion.consumers.aircraft_consumer.KafkaConsumer"
        ) as mock_kafka:

            mock_consumer = MagicMock()
            mock_consumer.__iter__ = MagicMock(side_effect=KeyboardInterrupt)
            mock_kafka.return_value = mock_consumer

            mock_cursor = MagicMock()
            mock_conn.return_value.cursor.return_value = mock_cursor

            from ingestion.consumers.aircraft_consumer import main

            main()

            mock_cursor.close.assert_called()


class TestLagMonitorReport:
    """Tests for lag monitor report function."""

    def test_report_exits_on_keyboard_interrupt(self):
        """report exits cleanly on KeyboardInterrupt."""
        with patch("ingestion.consumers.lag_monitor.get_lag") as mock_lag:
            mock_lag.side_effect = [
                {0: {"lag": 0, "committed_offset": 10, "end_offset": 10}},
                {0: {"lag": 0, "committed_offset": 6, "end_offset": 6}},
                KeyboardInterrupt,
            ]
            from ingestion.consumers.lag_monitor import report

            report(interval=0)

    def test_report_calls_get_lag_for_both_groups(self):
        """report calls get_lag for vessel and aircraft groups."""
        call_count = {"count": 0}

        def mock_lag(*args, **kwargs):
            call_count["count"] += 1
            if call_count["count"] > 2:
                raise KeyboardInterrupt
            return {0: {"lag": 0, "committed_offset": 10, "end_offset": 10}}

        with patch(
            "ingestion.consumers.lag_monitor.get_lag", side_effect=mock_lag
        ), patch("ingestion.consumers.lag_monitor.time.sleep"):
            from ingestion.consumers.lag_monitor import report

            report(interval=0)

        assert call_count["count"] >= 2


class TestAircraftConsumerMainLoop:
    """Tests for aircraft consumer main loop."""

    def test_main_commits_on_success(self):
        """main commits after successful insert."""

        aircraft_msg = {
            "icao24": "8a02ca",
            "callsign": "AWQ504",
            "origin_country": "Indonesia",
            "latitude": 1.1998,
            "longitude": 103.915,
            "altitude_m": 777.24,
            "velocity_ms": 93.83,
            "heading": 202.91,
            "vertical_rate": 8.13,
            "on_ground": False,
            "squawk": None,
            "source": "opensky",
            "timestamp": "2026-04-08T18:25:04+00:00",
        }

        mock_message = MagicMock()
        mock_message.value = aircraft_msg

        with patch(
            "ingestion.consumers.aircraft_consumer.build_connection"
        ) as mock_conn, patch(
            "ingestion.consumers.aircraft_consumer.KafkaConsumer"
        ) as mock_kafka:

            mock_consumer = MagicMock()
            mock_consumer.__iter__ = MagicMock(return_value=iter([mock_message]))
            mock_kafka.return_value = mock_consumer

            mock_cursor = MagicMock()
            mock_conn.return_value.cursor.return_value = mock_cursor

            from ingestion.consumers.aircraft_consumer import main

            main()

            mock_conn.return_value.commit.assert_called()
