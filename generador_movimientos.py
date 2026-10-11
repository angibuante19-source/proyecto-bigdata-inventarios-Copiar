
import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

TOPIC = "movimientos_inventario"

productos = [
    {"producto": "Teclado", "precio": 45000},
    {"producto": "Mouse", "precio": 25000},
    {"producto": "Monitor", "precio": 650000},
    {"producto": "Portatil", "precio": 2200000},
    {"producto": "Cable HDMI", "precio": 18000},
]

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda dato: json.dumps(dato).encode("utf-8"),
)

print("Generador de movimientos iniciado.")
print("Presiona Ctrl+C para detenerlo.")

try:
    while True:
        producto = random.choice(productos)
        tipo = random.choice(["ENTRADA", "VENTA"])
        cantidad = random.randint(1, 5)

        evento = {
            "fecha": datetime.now().isoformat(),
            "producto": producto["producto"],
            "tipo": tipo,
            "cantidad": cantidad,
            "precio_unitario": producto["precio"],
        }

        producer.send(TOPIC, value=evento)
        producer.flush()

        print("Movimiento enviado:", evento)
        time.sleep(3)

except KeyboardInterrupt:
    print("\nGenerador detenido.")

finally:
    producer.close()
