import argparse
import json
import random
import sys
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config.kafka_settings import TOPIC, build_connection_settings

SENSOR_KEY = b"sensor1"
WIND_DIRECTIONS = ("N", "NO", "O", "SO", "S", "SE", "E", "NE")

READING_MEAN = 55
READING_STANDARD_DEVIATION = 15
TEMPERATURE_RANGE_CELSIUS = (0, 110)
HUMIDITY_RANGE_PERCENT = (0, 100)
SEND_INTERVAL_RANGE_SECONDS = (15, 30)
BROKER_TIMEOUT_MILLISECONDS = 10000
SEND_CONFIRMATION_TIMEOUT_SECONDS = 15
PRODUCER_CLOSE_TIMEOUT_SECONDS = 5

# Payload compacto de 24 bits: temperatura en centesimas de grado (14 bits) | humedad (7 bits) | viento (3 bits)
COMPACT_PAYLOAD_BYTES = 3
TEMPERATURE_BIT_OFFSET = 10
HUMIDITY_BIT_OFFSET = 3
HUMIDITY_BIT_MASK = 0b1111111
WIND_DIRECTION_BIT_MASK = 0b111


def sample_normal_within_range(value_range, decimals):
    minimum_value, maximum_value = value_range
    while True:
        sampled_value = round(random.gauss(READING_MEAN, READING_STANDARD_DEVIATION), decimals)
        if minimum_value <= sampled_value <= maximum_value:
            return sampled_value


def generate_measurement():
    return {
        "temperature": sample_normal_within_range(TEMPERATURE_RANGE_CELSIUS, decimals=2),
        "humidity": int(sample_normal_within_range(HUMIDITY_RANGE_PERCENT, decimals=0)),
        "wind_direction": random.choice(WIND_DIRECTIONS),
    }


def encode_compact(measurement):
    temperature_centidegrees = round(measurement["temperature"] * 100)
    humidity_percent = int(measurement["humidity"])
    wind_direction_index = WIND_DIRECTIONS.index(measurement["wind_direction"])

    minimum_temperature, maximum_temperature = TEMPERATURE_RANGE_CELSIUS
    if not minimum_temperature * 100 <= temperature_centidegrees <= maximum_temperature * 100:
        raise ValueError(f"Temperatura fuera del rango {TEMPERATURE_RANGE_CELSIUS} °C")
    if not HUMIDITY_RANGE_PERCENT[0] <= humidity_percent <= HUMIDITY_RANGE_PERCENT[1]:
        raise ValueError(f"Humedad fuera del rango {HUMIDITY_RANGE_PERCENT} %")

    packed_measurement = (
        (temperature_centidegrees << TEMPERATURE_BIT_OFFSET)
        | (humidity_percent << HUMIDITY_BIT_OFFSET)
        | wind_direction_index
    )
    return packed_measurement.to_bytes(COMPACT_PAYLOAD_BYTES, byteorder="big")


def decode_compact(payload):
    if len(payload) != COMPACT_PAYLOAD_BYTES:
        raise ValueError(f"El payload compacto debe tener exactamente {COMPACT_PAYLOAD_BYTES} bytes")

    packed_measurement = int.from_bytes(payload, byteorder="big")
    return {
        "temperature": (packed_measurement >> TEMPERATURE_BIT_OFFSET) / 100,
        "humidity": (packed_measurement >> HUMIDITY_BIT_OFFSET) & HUMIDITY_BIT_MASK,
        "wind_direction": WIND_DIRECTIONS[packed_measurement & WIND_DIRECTION_BIT_MASK],
    }


def encode_measurement(measurement, message_format):
    if message_format == "compact":
        payload = encode_compact(measurement)
        if decode_compact(payload) != measurement:
            raise ValueError("La medición cambió al codificarla en 3 bytes")
        return payload
    return json.dumps(measurement).encode("utf-8")


def describe_payload(payload, message_format):
    return payload.hex() if message_format == "compact" else payload.decode("utf-8")


def parse_arguments():
    argument_parser = argparse.ArgumentParser(
        description="Simula una estación meteorológica y publica sus lecturas en Kafka."
    )
    argument_parser.add_argument("--format", choices=("json", "compact"), default="json")
    argument_parser.add_argument("--dry-run", action="store_true", help="Muestra una lectura sin conectar con Kafka")
    argument_parser.add_argument("--once", action="store_true", help="Envía una sola lectura y termina")
    return argument_parser.parse_args()


def print_dry_run(message_format):
    measurement = generate_measurement()
    payload = encode_measurement(measurement, message_format)
    print(f"Topic: {TOPIC} | Key: {SENSOR_KEY.decode('utf-8')}")
    print(f"Medición: {measurement}")
    print(f"Payload: {describe_payload(payload, message_format)}")
    print(f"Tamaño: {len(payload)} bytes")


def create_producer():
    return KafkaProducer(
        **build_connection_settings(),
        api_version_auto_timeout_ms=BROKER_TIMEOUT_MILLISECONDS,
        max_block_ms=BROKER_TIMEOUT_MILLISECONDS,
    )


def publish_measurements(producer, message_format, send_only_once):
    while True:
        measurement = generate_measurement()
        payload = encode_measurement(measurement, message_format)
        delivery_metadata = producer.send(TOPIC, key=SENSOR_KEY, value=payload).get(
            timeout=SEND_CONFIRMATION_TIMEOUT_SECONDS
        )
        print(
            f"Enviado: {measurement} | {len(payload)} bytes | "
            f"partición {delivery_metadata.partition}, offset {delivery_metadata.offset}"
        )

        if send_only_once:
            return
        time.sleep(random.randint(*SEND_INTERVAL_RANGE_SECONDS))


def run_producer():
    arguments = parse_arguments()
    if arguments.dry_run:
        print_dry_run(arguments.format)
        return 0

    producer = None
    try:
        producer = create_producer()
        print(
            f"Enviando al topic {TOPIC} con key {SENSOR_KEY.decode('utf-8')} ({arguments.format}). "
            "Ctrl+C para detener."
        )
        publish_measurements(producer, arguments.format, arguments.once)
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
