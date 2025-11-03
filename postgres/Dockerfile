FROM postgres:13

RUN apt-get update && apt-get install -y python3 python3-pip python3-venv

COPY requirements.txt /tmp/requirements.txt
RUN python3 -m venv /opt/venv && \
    /opt/venv/bin/pip install -r /tmp/requirements.txt

COPY init-scripts/ /docker-entrypoint-initdb.d/
COPY data_generator.py /opt/data_generator.py