FROM prefecthq/prefect:3-latest
# Install Docker CLI
RUN apt-get update && apt-get install -y docker.io

WORKDIR /opt/prefect