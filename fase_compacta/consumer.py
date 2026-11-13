import json
from datetime import datetime
from pathlib import Path

from kafka import KafkaConsumer

from consumer.live_plotter import LiveWeatherPlotter
from fase_compacta_local.local_kafka_settings import BOOTSTRAP_SERVER, TOPIC, build_connection_settings
from fase_compacta_local.payload_codec import InvalidPayloadError, decode

CONSUMER_GROUP_ID = f"{TOPIC}-weather-dashboard-compacto"
POLL_TIMEOUT_MILLISECONDS = 500
WINDOW_REFRESH_SECONDS = 0.2
SNAPSHOT_PATH = Path(__file__).resolve().parent / "evidencia" / "grafica_compacta_tiempo_real.png"


def create_consumer():
    return KafkaConsumer(
        TOPIC,
        **build_connection_settings(),
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )


def process_record(record, plotter):
    reading_time = datetime.fromtimestamp(record.timestamp / 1000)
    try:
        reading = json.loads(decode(record.value))
        plotter.add_reading(reading, reading_time)
    except (InvalidPayloadError, ValueError) as invalid_reading_error:
        print(f"[descartado] particion {record.partition} offset {record.offset}: {invalid_reading_error}")
        return False

    print(
        f"[{reading_time:%H:%M:%S}] particion {record.partition} offset {record.offset} | "
        f"0x{record.value.hex()} ({len(record.value)} bytes) -> {reading}"
    )
    return True


def is_already_processed(record, last_offset_by_partition):
    return record.offset <= last_offset_by_partition.get(record.partition, -1)


def consume_available_records(consumer, plotter, last_offset_by_partition):
    records_by_partition = consumer.poll(timeout_ms=POLL_TIMEOUT_MILLISECONDS)
    accepted_records_count = 0
    for partition_records in records_by_partition.values():
        for record in partition_records:
            if is_already_processed(record, last_offset_by_partition):
                continue
            last_offset_by_partition[record.partition] = record.offset
            if process_record(record, plotter):
                accepted_records_count += 1
    return accepted_records_count


def run_consumer():
    consumer = create_consumer()
    plotter = LiveWeatherPlotter(station_title=f"Estación meteorológica · payload 3 bytes · broker local · topic {TOPIC}")
    last_offset_by_partition = {}
    SNAPSHOT_PATH.parent.mkdir(exist_ok=True)
    print(f"Escuchando topic '{TOPIC}' en {BOOTSTRAP_SERVER}. Cierra la ventana o presiona Ctrl+C para terminar.")

    try:
        while plotter.is_window_open():
            if consume_available_records(consumer, plotter, last_offset_by_partition):
                plotter.refresh()
                plotter.save_snapshot(SNAPSHOT_PATH)
            plotter.keep_window_responsive(WINDOW_REFRESH_SECONDS)
    except KeyboardInterrupt:
        print("\nConsumer detenido por el usuario.")
    finally:
        consumer.close()


run_consumer()
