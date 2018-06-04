FROM python:3.6-stretch

RUN pip3 install git+https://github.com/Cobliteam/kafka-topic-dumper.git

WORKDIR /opt/kafka-topic-dumper

VOLUME /data

ENTRYPOINT ["kafka-topic-dumper"]
CMD ["kafka-topic-dumper", "-h"]
