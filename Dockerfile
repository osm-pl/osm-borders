FROM python:3.6-slim

COPY requirements.txt /app/
RUN apt-get -y update && \
    apt-get install -y \
        g++ \
        gcc \
        libgdal1h \
        libgdal-dev \
        libgeos-3.4.2 \
        libgeos-c1 \
        libgeos-dev \
    && \
    pip install -r /app/requirements.txt && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf ~/.pip && \ 
    apt remove \
        g++ \
        gcc \
        libgdal-dev \
        libgeos-dev \
    && \
    apt-get autoremove -y
COPY . /app/
EXPOSE 5002
VOLUME /data
ENV TMPDIR=/data
CMD python /app/rest_server.py
