FROM centos:8

ARG version
ARG pipeline_args

LABEL pipeline=$pipeline_args

MAINTAINER pteyer@salesforce.com

ENV DEBIAN_FRONTEND noninteractive

RUN groupadd --system --gid 7447 cantor
RUN adduser --system --gid 7447 --uid 7447 --shell /sbin/nologin cantor

RUN yum install java -y

# work in cantor home directory
WORKDIR /home/cantor/

# add start script and cantor jar
ADD start.sh /home/cantor/start.sh
ADD cantor-server.jar /home/cantor/cantor-server.jar
ADD cantor-server.conf /home/cantor/cantor-server.conf
ADD cantor-logback.xml /home/cantor/cantor-logback.xml

# make cantor the owner
RUN chown -R cantor /home/cantor/

CMD bash ./start.sh /home/cantor/cantor-server.conf

