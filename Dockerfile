FROM python:3.7-slim

COPY requirements.txt /app/
RUN apt-get -y update && \
    apt-get install -y \
        g++ \
        gcc \
        libgdal20 \
        libgdal-dev \
        libgeos-3.5.1 \
        libgeos-c1v5 \
        libgeos-dev \
    && \
    pip install -r /app/requirements.txt && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf ~/.pip && \ 
    apt remove -y \
        g++ \
        gcc \
        libgdal-dev \
        libgeos-dev \
    && \
    apt-get autoremove -y
COPY . /app/
RUN mkdir /data /output
EXPOSE 5002
ENV TMPDIR=/data
WORKDIR /output
ENTRYPOINT ["python", "/app/docker_entrypoint.py"]
CMD ["--server"]
