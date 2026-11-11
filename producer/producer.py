"""Simula una estación meteorológica y publica sus lecturas en Kafka."""

import argparse
import getpass
import json
import os
import random
import sys
import time


BROKER = "161.35.11.141:9092"
TOPIC = "23542"
KEY = b"sensor1"
WIND_DIRECTIONS = ("N", "NO", "O", "SO", "S", "SE", "E", "NE")


def normal_in_range(minimum, maximum, decimals):
    """Genera una lectura cercana a 55, descartando valores fuera del rango."""
    while True:
        value = round(random.gauss(55, 15), decimals)
        if minimum <= value <= maximum:
            return value


def generate_measurement():
    return {
        "temperature": normal_in_range(0, 110, 2),
        "humidity": int(normal_in_range(0, 100, 0)),
        "wind_direction": random.choice(WIND_DIRECTIONS),
    }


def encode_compact(measurement):
    """Empaca temperatura (14 bits), humedad (7) y viento (3) en 3 bytes."""
    temperature_centi = round(measurement["temperature"] * 100)
    humidity = int(measurement["humidity"])
    direction = WIND_DIRECTIONS.index(measurement["wind_direction"])

    if not 0 <= temperature_centi <= 11000:
        raise ValueError("Temperatura fuera del rango 0..110 °C")
    if not 0 <= humidity <= 100:
        raise ValueError("Humedad fuera del rango 0..100 %")

    packed = (temperature_centi << 10) | (humidity << 3) | direction
    return packed.to_bytes(3, byteorder="big")


def decode_compact(payload):
    """Reconstruye la medición de un payload de exactamente 3 bytes."""
    if len(payload) != 3:
        raise ValueError("El payload compacto debe tener exactamente 3 bytes")

    packed = int.from_bytes(payload, byteorder="big")
    return {
        "temperature": (packed >> 10) / 100,
        "humidity": (packed >> 3) & 0x7F,
        "wind_direction": WIND_DIRECTIONS[packed & 0x07],
    }


def encode_measurement(measurement, message_format):
    if message_format == "compact":
        payload = encode_compact(measurement)
        if decode_compact(payload) != measurement:
            raise ValueError("La medición cambió al codificarla en 3 bytes")
        return payload
    return json.dumps(measurement).encode("utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--format", choices=("json", "compact"), default="json")
    parser.add_argument("--dry-run", action="store_true", help="Muestra una lectura sin conectar con Kafka")
    parser.add_argument("--once", action="store_true", help="Envía una sola lectura y termina")
    args = parser.parse_args()

    if args.dry_run:
        measurement = generate_measurement()
        payload = encode_measurement(measurement, args.format)
        print(f"Topic: {TOPIC} | Key: {KEY.decode('utf-8')}")
        print(f"Medición: {measurement}")
        print(f"Payload: {payload.hex() if args.format == 'compact' else payload.decode('utf-8')}")
        print(f"Tamaño: {len(payload)} bytes")
        return 0

    try:
        from kafka import KafkaProducer
    except ImportError:
        print("Falta kafka-python. Instala las dependencias de requirements.txt.", file=sys.stderr)
        return 1

    password = os.environ.get("KAFKA_PASSWORD") or getpass.getpass("Contraseña de Kafka: ")
    if not password:
        print("Se necesita la contraseña de Kafka.", file=sys.stderr)
        return 1

    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=[BROKER],
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_plain_username="estudiante",
            sasl_plain_password=password,
            api_version_auto_timeout_ms=10000,
            max_block_ms=10000,
        )
        print(f"Enviando al topic {TOPIC} con key {KEY.decode('utf-8')} ({args.format}). Ctrl+C para detener.")

        while True:
            measurement = generate_measurement()
            payload = encode_measurement(measurement, args.format)
            metadata = producer.send(TOPIC, key=KEY, value=payload).get(timeout=15)
            print(f"Enviado: {measurement} | {len(payload)} bytes | partición {metadata.partition}, offset {metadata.offset}")

            if args.once:
                break
            time.sleep(random.randint(15, 30))

    except KeyboardInterrupt:
        print("\nEnvío detenido.")
    except Exception as exc:
        print(f"No se pudo enviar la lectura: {exc}", file=sys.stderr)
        return 1
    finally:
        if producer is not None:
            producer.close(timeout=5)

    return 0


if __name__ == "__main__":
    sys.exit(main())
