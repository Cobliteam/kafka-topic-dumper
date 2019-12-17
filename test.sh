#!/bin/sh

set -e

flake8 kafka_topic_dumper
pytest --cov=kafka_topic_dumper --cov-fail-under=0 kafka_topic_dumper/test $@

