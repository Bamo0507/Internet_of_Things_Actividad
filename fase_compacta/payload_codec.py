import json

WIND_DIRECTIONS = ("N", "NO", "O", "SO", "S", "SE", "E", "NE")
TEMPERATURE_RANGE_CELSIUS = (0, 110)
HUMIDITY_RANGE_PERCENT = (0, 100)

# 24 bits: temperatura en centésimas de grado (14 bits) | humedad (7 bits) | índice de dirección del viento (3 bits)
COMPACT_PAYLOAD_BYTES = 3
TEMPERATURE_BIT_OFFSET = 10
HUMIDITY_BIT_OFFSET = 3
HUMIDITY_BIT_MASK = 0b1111111
WIND_DIRECTION_BIT_MASK = 0b111


class InvalidPayloadError(ValueError):
    pass


def encode(measurement_json):
    measurement = json.loads(measurement_json)
    temperature_centidegrees = round(measurement["temperature"] * 100)
    humidity_percent = int(measurement["humidity"])
    wind_direction_index = WIND_DIRECTIONS.index(measurement["wind_direction"])

    minimum_temperature, maximum_temperature = TEMPERATURE_RANGE_CELSIUS
    if not minimum_temperature * 100 <= temperature_centidegrees <= maximum_temperature * 100:
        raise InvalidPayloadError(f"Temperatura fuera del rango {TEMPERATURE_RANGE_CELSIUS} °C")
    if not HUMIDITY_RANGE_PERCENT[0] <= humidity_percent <= HUMIDITY_RANGE_PERCENT[1]:
        raise InvalidPayloadError(f"Humedad fuera del rango {HUMIDITY_RANGE_PERCENT} %")

    packed_measurement = (
        (temperature_centidegrees << TEMPERATURE_BIT_OFFSET)
        | (humidity_percent << HUMIDITY_BIT_OFFSET)
        | wind_direction_index
    )
    return packed_measurement.to_bytes(COMPACT_PAYLOAD_BYTES, byteorder="big")


def decode(payload):
    if len(payload) != COMPACT_PAYLOAD_BYTES:
        raise InvalidPayloadError(
            f"Se esperaban {COMPACT_PAYLOAD_BYTES} bytes y llegaron {len(payload)}: {payload!r}"
        )

    packed_measurement = int.from_bytes(payload, byteorder="big")
    return json.dumps({
        "temperature": (packed_measurement >> TEMPERATURE_BIT_OFFSET) / 100,
        "humidity": (packed_measurement >> HUMIDITY_BIT_OFFSET) & HUMIDITY_BIT_MASK,
        "wind_direction": WIND_DIRECTIONS[packed_measurement & WIND_DIRECTION_BIT_MASK],
    })
