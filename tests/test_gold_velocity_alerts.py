
from datetime import datetime, timedelta
from transformations.gold_velocity_alerts_transforms import detect_velocity_alert

def test_multiple_cities_in_window_triggers_alert(spark):
    base_time = datetime(2024, 1, 1, 10, 0, 0)
    data = [
        ("CUST001", "Dubai", base_time),
        ("CUST001", "Abu Dhabi", base_time + timedelta(minutes=2)),
    ]
    df = spark.createDataFrame(data, ["customer_id", "city", "swipe_timestamp"])

    result = detect_velocity_alerts(df)
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["distinct_city_count"] == 2
    assert rows[0]["swipe_count"] == 2


def test_single_city_does_not_trigger_alert(spark):
    base_time = datetime(2024, 1, 1, 10, 0, 0)
    data = [
        ("CUST001", "Dubai", base_time),
        ("CUST001", "Dubai", base_time + timedelta(minutes=1)),
    ]
    df = spark.createDataFrame(data, ["customer_id", "city", "swipe_timestamp"])

    result = detect_velocity_alert(df)
    assert result.count() == 0


def test_swipes_outside_window_not_grouped_together(spark):
    base_time = datetime(2024, 1, 1, 10, 0, 0)
    data = [
        ("CUST001", "Dubai", base_time),
        ("CUST001", "Abu Dhabi", base_time + timedelta(minutes=10)),  # 5-min window ke bahar
    ]
    df = spark.createDataFrame(data, ["customer_id", "city", "swipe_timestamp"])

    result = detect_velocity_alert(df)
    # Alag windows mein aa jayenge, koi bhi window mein 2 cities nahi honge
    assert result.count() == 0