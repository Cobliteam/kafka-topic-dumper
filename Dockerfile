FROM python:3.6-slim

WORKDIR /app

COPY . .

RUN pip3 install -e .

VOLUME /data

CMD ["kafka-topic-dumper", "-h"]
