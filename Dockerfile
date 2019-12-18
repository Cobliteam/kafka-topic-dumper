FROM python:3.6-slim

WORKDIR /app

COPY ./kafka_topic_dumper ./kafka_topic_dumper
COPY ./setup.cfg .
COPY ./setup.py .
COPY ./requirements.txt .

RUN pip3 install -e .

VOLUME /data

CMD ["kafka-topic-dumper", "-h"]
