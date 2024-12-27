FROM apache/airflow:2.10.4

# Add the airflow user to the docker group for access to the Docker socket
USER root
RUN groupadd -g 1001 docker && usermod -aG docker airflow
USER airflow