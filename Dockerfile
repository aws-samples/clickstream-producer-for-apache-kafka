FROM public.ecr.aws/docker/library/centos:latest

RUN cd /etc/yum.repos.d/
RUN sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*
RUN sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*

RUN yum install -y wget tar openssh-server openssh-clients sysstat sudo which openssl hostname
RUN yum install -y java-17-openjdk java-17-openjdk-devel 
RUN yum install -y epel-release &&\
    yum install -y jq &&\
    yum install -y nmap-ncat git
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV PATH=$JAVA_HOME/bin:$PATH
    
# Verify Java installation
RUN java -version && javac -version

# First install required dependencies
RUN yum groupinstall -y "Development Tools" && \
    yum install -y gcc openssl-devel bzip2-devel libffi-devel zlib-devel make


RUN yum install -y maven

ENV SCALA_VERSION 2.13
ENV KAFKA_VERSION 3.7.0

# Prometheus Java agent
RUN wget https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.13.0/jmx_prometheus_javaagent-0.13.0.jar
RUN mv jmx_prometheus_javaagent-0.13.0.jar /opt

RUN git clone https://github.com/aws-samples/sasl-scram-secrets-manager-client-for-msk.git
WORKDIR sasl-scram-secrets-manager-client-for-msk
RUN mvn clean install -f pom.xml
WORKDIR ../

# TODO: uncomment after test completes
RUN git clone https://github.com/aws-samples/clickstream-producer-for-apache-kafka.git
WORKDIR clickstream-producer-for-apache-kafka
RUN mvn clean install -f pom.xml

# add properties files 
ADD producer.properties /opt/producer.properties
ADD kafka-producer-consumer.yml /opt/kafka-producer-consumer.yml

RUN mv target/KafkaClickstreamClient-1.0-SNAPSHOT.jar /opt

# include all start scripts
ADD start-kafka-producer.sh /opt/start-kafka-producer.sh
RUN mkdir -p /opt/logs
RUN chmod 777 /opt/start-kafka-producer.sh

# cleanup
RUN yum clean all;

EXPOSE 3800
USER root
ENTRYPOINT /opt/start-kafka-producer.sh
