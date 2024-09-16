import redis
import hashlib
import time

client = redis.StrictRedis(host='localhost', port=6379, db=0)

# Función para agregar muchas claves
def populate_cache():
    for i in range(1, 20000):  # Ajusta este número según lo necesites
        key = f"key{i}"
        value = f"value{i}"
        client.set(key, value)
        print(f"Guardado: {key} -> {value}")
        time.sleep(0.0001)  # Añade un pequeño retraso si es necesario

populate_cache()
