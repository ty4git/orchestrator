ARG FILEBEAT_VERSION=8.13.4
FROM docker.elastic.co/beats/filebeat:${FILEBEAT_VERSION}

COPY --chown=root:root ./filebeat.yml /usr/share/filebeat/filebeat.yml