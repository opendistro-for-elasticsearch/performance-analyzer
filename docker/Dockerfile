# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

################################################################################
# Build stage 0 `prep_es_files`:
# Extract elasticsearch artifact
# Install required plugins
# Set gid=0 and make group perms==owner perms
################################################################################

FROM centos:7 AS prep_es_files

ENV PATH /usr/share/elasticsearch/bin:$PATH
RUN curl -s https://download.java.net/java/GA/jdk11/13/GPL/openjdk-11.0.1_linux-x64_bin.tar.gz | tar -C /opt -zxf -
ENV JAVA_HOME /opt/jdk-11.0.1
RUN yum install -y unzip
RUN yum install -y lsof

RUN groupadd -g 1000 elasticsearch && \
    adduser -u 1000 -g 1000 -d /usr/share/elasticsearch elasticsearch

USER 1000


WORKDIR /usr/share/elasticsearch

# Bust cache for wgets
ENV BUST_CACHE 1576286189

# Download and extract defined ES version.
RUN curl -fsSL https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-oss-7.7.0-darwin-x86_64.tar.gz | \
    tar zx --strip-components=1

RUN set -ex && for esdirs in config data logs; do \
        mkdir -p "$esdirs"; \
    done

COPY --chown=1000:0 elasticsearch.yml log4j2.properties config/

COPY --chown=1000:0 opendistro_performance_analyzer-1.8.0.0-SNAPSHOT.zip /tmp/

RUN elasticsearch-plugin install --batch file:///tmp/opendistro_performance_analyzer-1.8.0.0-SNAPSHOT.zip; \
        rm /tmp/opendistro_performance_analyzer-1.8.0.0-SNAPSHOT.zip

USER 0

# Set gid to 0 for opendistroforelasticsearch and make group permission similar to that of user
RUN chown -R elasticsearch:0 . && \
    chmod -R g=u /usr/share/elasticsearch

RUN chmod 755 /usr/share/elasticsearch/plugins/opendistro_performance_analyzer/pa_bin/performance-analyzer-agent
RUN chmod -R 755 /dev/shm
################################################################################
# Build stage 1 (the actual opendistroforelasticsearch image):
# Copy opendistroforelasticsearch from stage 0
# Add entrypoint
################################################################################

FROM centos:7

ENV ELASTIC_CONTAINER true

RUN \
  rpm --rebuilddb && yum clean all && \
  yum install -y epel-release && \
  yum update -y && \
  yum install -y \
                  iproute \
                  python-setuptools \
                  hostname \
                  inotify-tools \
                  yum-utils \
                  which \
                  jq \
                  lsof \
                  rsync && \
  yum clean all && \
  easy_install supervisor

RUN yum update -y && \
    yum install -y nc unzip wget which && \
    yum clean all
COPY CENTOS_LICENSING.txt /root
COPY --from=prep_es_files --chown=1000:0 /opt/jdk-11.0.1 /opt/jdk-11.0.1
ENV JAVA_HOME /opt/jdk-11.0.1

# Replace OpenJDK's built-in CA certificate keystore with the one from the OS
# vendor. The latter is superior in several ways.
RUN ln -sf /etc/pki/ca-trust/extracted/java/cacerts /opt/jdk-11.0.1/lib/security/cacerts

ENV PATH $PATH:$JAVA_HOME/bin

RUN mkdir /usr/share/elasticsearch && \
    groupadd -g 1000 elasticsearch && \
    adduser -u 1000 -g 1000 -G 0 -d /usr/share/elasticsearch elasticsearch && \
    chmod 0775 /usr/share/elasticsearch && \
    chgrp 0 /usr/share/elasticsearch

RUN mkdir /usr/share/supervisor

WORKDIR /usr/share/elasticsearch
COPY --from=prep_es_files --chown=1000:0 /usr/share/elasticsearch /usr/share/elasticsearch
ENV PATH /usr/share/elasticsearch/bin:$PATH

ADD --chown=1000:0 docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

# Openshift overrides USER and uses ones with randomly uid>1024 and gid=0
# Allow ENTRYPOINT (and ES) to run even with a different user
RUN chgrp 0 /usr/local/bin/docker-entrypoint.sh && \
    chmod g=u /etc/passwd && \
    chmod 0775 /usr/local/bin/docker-entrypoint.sh

EXPOSE 9200 9300 9600 9650

LABEL org.label-schema.schema-version="1.0" \
  org.label-schema.name="opendistroforelasticsearch" \
  org.label-schema.version="1.2.0" \
  org.label-schema.url="https://opendistro.github.io" \
  org.label-schema.vcs-url="https://github.com/opendistro-for-elasticsearch/opendistro-build" \
  org.label-schema.license="Apache-2.0" \
  org.label-schema.vendor="Amazon" \
  org.label-schema.build-date="19.12.13"

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
# Dummy overridable parameter parsed by entrypoint
CMD ["eswrapper"]
