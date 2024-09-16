import redis
import hashlib
import csv
import subprocess
import time

# Función para generar conexiones a Redis con 8 particiones (db0 a db7)
def create_partitions():
    partitions = [redis.StrictRedis(host='localhost', port=6379, db=i) for i in range(8)]
    return partitions

# Función para seleccionar la partición (base de datos) basada en un hash de la clave
def get_partition(domain, partitions):
    hash_value = int(hashlib.md5(domain.encode()).hexdigest(), 16)
    partition_index = hash_value % len(partitions)
    return partitions[partition_index]

# Función para resolver una consulta DNS usando el comando `dig`
def resolve_dns(domain):
    result = subprocess.run(['dig', '+short', domain], stdout=subprocess.PIPE)
    return result.stdout.decode('utf-8').strip()

# Función para obtener un valor del caché o resolverlo si no está
def cache_get(domain, partitions):
    partition = get_partition(domain, partitions)
    ip_address = partition.get(domain)
    
    if ip_address:
        print(f"Cache HIT en partición {partitions.index(partition)}: {domain} -> {ip_address.decode('utf-8')}")
        return ip_address.decode('utf-8')
    else:
        # Si no está en caché, resolver la consulta DNS
        print(f"Cache MISS: {domain}")
        ip_address = resolve_dns(domain)
        
        if ip_address:
            # Almacenar el resultado en la partición correspondiente
            partition.set(domain, ip_address)
            print(f"Guardado en partición {partitions.index(partition)}: {domain} -> {ip_address}")
        
        return ip_address

# Función para leer dominios del archivo 3rd_lev_domains.csv y generar tráfico en el caché
def generate_traffic_from_dataset(dataset_path, partitions):
    with open(dataset_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            domain = row[0]  # Supongo que la primera columna contiene los dominios
            cache_get(domain, partitions)
            time.sleep(0.01)  # Añadir un pequeño retraso entre cada solicitud para simular tráfico

# Ejemplo de uso
if __name__ == "__main__":
    # Crea 8 particiones (bases de datos db0 a db7)
    partitions = create_partitions()

    # Ruta a los archivos CSV del dataset
    dataset = "3rd_lev_domains.csv"

    # Generar tráfico desde el archivo del dataset
    print("Generando tráfico desde 3rd_lev_domains.csv...")
    generate_traffic_from_dataset(dataset, partitions)
