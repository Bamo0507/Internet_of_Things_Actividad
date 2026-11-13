import argparse
import json
import random
import sys
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

from fase_compacta_local.local_kafka_settings import BOOTSTRAP_SERVER, TOPIC, build_connection_settings
from fase_compacta_local.payload_codec import (
    HUMIDITY_RANGE_PERCENT,
    TEMPERATURE_RANGE_CELSIUS,
    WIND_DIRECTIONS,
    decode,
    encode,
)

SENSOR_KEY = b"sensor1"
READING_MEAN = 55
READING_STANDARD_DEVIATION = 15
SEND_INTERVAL_RANGE_SECONDS = (15, 30)
BROKER_TIMEOUT_MILLISECONDS = 10000
SEND_CONFIRMATION_TIMEOUT_SECONDS = 15
PRODUCER_CLOSE_TIMEOUT_SECONDS = 5


def sample_normal_within_range(value_range, decimals):
    minimum_value, maximum_value = value_range
    while True:
        sampled_value = round(random.gauss(READING_MEAN, READING_STANDARD_DEVIATION), decimals)
        if minimum_value <= sampled_value <= maximum_value:
            return sampled_value


def generate_measurement_json():
    return json.dumps({
        "temperature": sample_normal_within_range(TEMPERATURE_RANGE_CELSIUS, decimals=2),
        "humidity": int(sample_normal_within_range(HUMIDITY_RANGE_PERCENT, decimals=0)),
        "wind_direction": random.choice(WIND_DIRECTIONS),
    })


def encode_without_loss(measurement_json):
    payload = encode(measurement_json)
    if json.loads(decode(payload)) != json.loads(measurement_json):
        raise ValueError("La medición cambió al codificarla en 3 bytes")
    return payload


def parse_arguments():
    argument_parser = argparse.ArgumentParser(
        description="Estación meteorológica simulada que publica lecturas de 3 bytes en un Kafka local."
    )
    argument_parser.add_argument("--dry-run", action="store_true", help="Muestra una lectura sin conectar con Kafka")
    argument_parser.add_argument("--once", action="store_true", help="Envía una sola lectura y termina")
    return argument_parser.parse_args()


def print_dry_run():
    measurement_json = generate_measurement_json()
    payload = encode_without_loss(measurement_json)
    print(f"Topic: {TOPIC} | Key: {SENSOR_KEY.decode('utf-8')}")
    print(f"JSON original: {measurement_json} ({len(measurement_json.encode('utf-8'))} bytes)")
    print(f"Payload codificado: 0x{payload.hex()} ({len(payload)} bytes)")
    print(f"JSON decodificado: {decode(payload)}")


def create_producer():
    return KafkaProducer(
        **build_connection_settings(),
        api_version_auto_timeout_ms=BROKER_TIMEOUT_MILLISECONDS,
        max_block_ms=BROKER_TIMEOUT_MILLISECONDS,
    )


def publish_measurements(producer, send_only_once):
    while True:
        measurement_json = generate_measurement_json()
        payload = encode_without_loss(measurement_json)
        delivery_metadata = producer.send(TOPIC, key=SENSOR_KEY, value=payload).get(
            timeout=SEND_CONFIRMATION_TIMEOUT_SECONDS
        )
        print(
            f"Enviado: {measurement_json} -> 0x{payload.hex()} | {len(payload)} bytes | "
            f"partición {delivery_metadata.partition}, offset {delivery_metadata.offset}"
        )

        if send_only_once:
            return
        time.sleep(random.randint(*SEND_INTERVAL_RANGE_SECONDS))


def run_producer():
    arguments = parse_arguments()
    if arguments.dry_run:
        print_dry_run()
        return 0

    producer = None
    try:
        producer = create_producer()
        print(
            f"Enviando payloads de 3 bytes a {BOOTSTRAP_SERVER}, topic {TOPIC}, "
            f"key {SENSOR_KEY.decode('utf-8')}. Ctrl+C para detener."
        )
        publish_measurements(producer, arguments.once)
    except KeyboardInterrupt:
        print("\nEnvío detenido.")
    except KafkaError as kafka_error:
        print(f"No se pudo enviar la lectura: {kafka_error}", file=sys.stderr)
        return 1
    finally:
        if producer is not None:
            producer.close(timeout=PRODUCER_CLOSE_TIMEOUT_SECONDS)

    return 0


sys.exit(run_producer())
