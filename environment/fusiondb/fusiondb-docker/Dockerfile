FROM openjdk:8u242-jre

WORKDIR /opt

# FusionDB version will be passed in at build time
ARG FUSIONDB_VERSION
ENV FUSIONDB_HOME=/opt/fusiondb-${FUSIONDB_VERSION}

# Update the base image OS and install wget and python
RUN apt-get update
RUN apt-get install -y wget python less

COPY build/presto-server-*.tar.gz ./

RUN rm -rf ${FUSIONDB_HOME}
RUN tar -zxf presto-server-*.tar.gz  && rm -f presto-server-*.tar.gz && mv presto-server-* fusiondb-${FUSIONDB_VERSION}

COPY etc ${FUSIONDB_HOME}/etc

RUN groupadd -r fdb --gid=1000 && \
    useradd -r -g fdb --uid=1000 -d ${FUSIONDB_HOME} fdb && \
    chown fdb:fdb -R ${FUSIONDB_HOME}

RUN cp ${FUSIONDB_HOME}/lib/presto-cli-*.jar /usr/local/bin/presto
RUN chmod +x /usr/local/bin/presto
RUN mkdir /data && chown fdb:fdb /data

WORKDIR ${FUSIONDB_HOME}

USER fdb
EXPOSE 8080
EXPOSE 8306

ENTRYPOINT ${FUSIONDB_HOME}/bin/launcher run
