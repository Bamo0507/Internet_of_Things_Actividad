import os

BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "161.35.11.141:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "23542")

SECURITY_PROTOCOL = "SASL_PLAINTEXT"
SASL_MECHANISM = "PLAIN"
SASL_USERNAME = os.getenv("KAFKA_USERNAME", "estudiante")
SASL_PASSWORD = os.getenv("KAFKA_PASSWORD", "TryluRandomPass-2!@!")


def build_connection_settings():
    return {
        "bootstrap_servers": BOOTSTRAP_SERVER,
        "security_protocol": SECURITY_PROTOCOL,
        "sasl_mechanism": SASL_MECHANISM,
        "sasl_plain_username": SASL_USERNAME,
        "sasl_plain_password": SASL_PASSWORD,
    }
