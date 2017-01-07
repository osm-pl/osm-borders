FROM python:3.6-slim

COPY requirements.txt /app/
RUN apt-get -y update && \
    apt-get install -y libgeos-dev proj-data gcc && \
    pip install -r /app/requirements.txt && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf ~/.pip && apt remove gcc && \
    apt-get autoremove -y
COPY . /app/
EXPOSE 5002
VOLUME /data
ENV TMPDIR=/data
CMD python /app/rest_server.py
