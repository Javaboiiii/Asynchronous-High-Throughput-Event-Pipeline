# Asynchronous-High-Throughput-Event-Pipeline


# 1. Stop containers and explicitly force Podman to destroy all anonymous/internal layers
podman-compose down --volumes --remove-orphans

# 2. Force delete any lingering container caches from Podman's system memory
podman rm -f kafka-1 kafka-2 kafka-3

# 3. Boot them back up completely pristine
podman-compose up -d
