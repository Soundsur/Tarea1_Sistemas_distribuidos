import redis
import hashlib
import subprocess

# Función para generar conexiones a Redis con N particiones
def create_partitions(n_partitions):
    partitions = [redis.StrictRedis(host='localhost', port=6379, db=i) for i in range(n_partitions)]
    return partitions

# Función para seleccionar la partición basada en un hash de la clave
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

# Ejemplo de uso
if __name__ == "__main__":
    # Configuración del número de particiones (2, 4, 8)
    n_partitions = 4  # Cambia este valor a 2, 4 o 8 según sea necesario
    partitions = create_partitions(n_partitions)

    domain = "google.com"
    result = cache_get(domain, partitions)
    print(f"Dirección IP de {domain}: {result}")
