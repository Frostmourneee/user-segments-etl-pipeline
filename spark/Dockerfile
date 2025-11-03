FROM apache/spark:3.5.0

USER root

RUN mkdir -p /opt/spark/jars
RUN curl -L -o /opt/spark/jars/postgresql-42.6.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

USER 185