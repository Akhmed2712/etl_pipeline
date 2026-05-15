FROM prefecthq/prefect:3-latest

# 1. Install Docker CLI sebagai root
USER root
#RUN apt-get update && \
#    apt-get install -y docker.io && \
#    rm -rf /var/lib/apt/lists/*

# 2. Masukkan user 'prefect' ke grup docker
# Ini agar user prefect bisa mengakses /var/run/docker.sock
RUN usermod -aG docker prefect

# 3. Set working directory dan berikan izin ke user prefect
WORKDIR /opt/prefect/projects
RUN chown -R root:root /opt/prefect/projects

# 4. Switch kembali ke user prefect
USER root