# System Modules
import os
import runpy
import sys

# Installed Modules
import pytest

# Project Modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from calculations import area_of_circle, get_nth_fibonacci  # noqa: E402
from src.aiops_pipeline import run_pipeline
from src.event_producer import EventProducer
from src.event_topic import EventTopic


def test_area_of_circle_positive_radius():
    """Test with a positive radius."""
    # Arrange
    radius = 1

    # Act
    result = area_of_circle(radius)

    # Assert
    assert abs(result - 3.14159) < 1e-5


def test_area_of_circle_zero_radius():
    """Test with a radius of zero."""
    # Arrange
    radius = 0

    # Act
    result = area_of_circle(radius)

    # Assert
    assert result == 0


def test_get_nth_fibonacci_zero():
    """Test with n=0."""
    # Arrange
    n = 0

    # Act
    result = get_nth_fibonacci(n)

    # Assert
    assert result == 0


def test_get_nth_fibonacci_one():
    """Test with n=1."""
    # Arrange
    n = 1

    # Act
    result = get_nth_fibonacci(n)

    # Assert
    assert result == 1


def test_area_of_circle_negative_radius_raises():
    """Test invalid negative radius."""
    with pytest.raises(ValueError, match="Radius cannot be negative"):
        area_of_circle(-1)


def test_get_nth_fibonacci_negative_raises():
    """Test invalid negative index."""
    with pytest.raises(ValueError, match="n cannot be negative"):
        get_nth_fibonacci(-1)


def test_get_nth_fibonacci_ten():
    """Test with n=10."""
    assert get_nth_fibonacci(10) == 55


def test_run_pipeline_detects_anomalies_and_counts_records():
    """Test the pipeline processes the telemetry file and returns detected anomalies."""
    result = run_pipeline("data/service_data.json")

    assert result["records_processed"] == 10
    assert len(result["anomalies_detected"]) == 2
    assert result["events_consumed"] == []


def test_warning_log_is_detected_as_anomaly():
    """Test that warning logs are treated as anomaly conditions."""
    from src.anomaly_detector import AnomalyDetector

    detector = AnomalyDetector()
    record = {
        "timestamp": "2026-09-20T10:10:00",
        "service": "auth-service",
        "response_time_ms": 110,
        "cpu_percent": 45,
        "memory_percent": 46,
        "log_level": "WARNING",
        "message": "A warning was logged"
    }

    event = detector.detect(record)

    assert event is not None
    assert "Error log detected" in event["reasons"]


def test_run_pipeline_main_block_executes_without_error():
    """Test the script entry point prints pipeline results."""
    original_cwd = os.getcwd()
    try:
        os.chdir(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
        runpy.run_path("src/aiops_pipeline.py", run_name="__main__")
    finally:
        os.chdir(original_cwd)


def test_event_producer_rejects_empty_event():
    """Test empty payloads are not published."""
    topic = EventTopic("empty-events")
    producer = EventProducer(topic)

    assert producer.publish({}) is False
    assert topic.get_messages() == []


def test_event_topic_clear_removes_messages():
    """Test event topic messages can be cleared."""
    topic = EventTopic("cleared-events")
    topic.publish({"type": "ANOMALY"})

    topic.clear()

    assert topic.get_messages() == []
