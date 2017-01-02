FROM python:3.6-slim

COPY requirements.txt /app/
RUN apt-get -y update && apt-get install -y libgeos-dev && pip install -r /app/requirements.txt
COPY . /app/
EXPOSE 5002

CMD python /app/rest_server.py
