import json

REQUIRED_FIELDS = ("temperature", "humidity", "wind_direction")


class InvalidMessageError(ValueError):
    pass


def decode_json_message(raw_message_value):
    try:
        decoded_message = json.loads(raw_message_value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as decoding_error:
        raise InvalidMessageError(f"Mensaje no es JSON valido: {raw_message_value!r}") from decoding_error

    if not isinstance(decoded_message, dict):
        raise InvalidMessageError(f"Se esperaba un objeto JSON, se recibio: {decoded_message!r}")

    missing_fields =[field for field in REQUIRED_FIELDS if field not in decoded_message]
    if missing_fields:
        raise InvalidMessageError(f"Faltan campos {missing_fields} en el mensaje: {decoded_message}")

    return {field: decoded_message[field] for field in REQUIRED_FIELDS}
