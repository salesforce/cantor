FROM centos:8

ARG version
ARG pipeline_args

LABEL pipeline=$pipeline_args

MAINTAINER pteyer@salesforce.com

ENV DEBIAN_FRONTEND noninteractive

RUN groupadd --system --gid 7447 cantor
RUN adduser --system --gid 7447 --uid 7447 --shell /sbin/nologin sfdc-cantor

# install Java (this should be as high as possible, it's a long layer so rebuilding it hurts...)
RUN yum install java -y

# work in cantor home directory
WORKDIR /home/sfdc-cantor/

# add start script and cantor jar
ADD start.sh /home/sfdc-cantor/start.sh
ADD cantor-server.jar /home/sfdc-cantor/cantor-server.jar
ADD cantor-server.conf /home/sfdc-cantor/cantor-server.conf
ADD cantor-logback.xml /home/sfdc-cantor/cantor-logback.xml

# make cantor the owner
RUN chown -R sfdc-cantor /home/sfdc-cantor/

CMD bash ./start.sh /home/sfdc-cantor/cantor-server.conf

