FROM apache/airflow:2.2.3-python3.8
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip


COPY airflow.cfg /opt/airflow/airflow.cfg

USER root

RUN apt-get update && apt-get install wget -y
RUN wget https://github.com/AdoptOpenJDK/openjdk8-upstream-binaries/releases/download/jdk8u252-b09/OpenJDK8U-jdk_x64_linux_8u252b09.tar.gz -O /tmp/jdk8.tar.gz
RUN mkdir /usr/lib/jdk && tar xfv /tmp/jdk8.tar.gz -C /usr/lib/jdk && rm /tmp/jdk8.tar.gz

ENV JAVA_HOME=/usr/lib/jdk/openjdk-8u252-b09
ENV PATH="/usr/lib/jdk/openjdk-8u252-b09/bin:${PATH}"

USER airflow

RUN pip install --upgrade pip==21.1.1
RUN pip install --no-cache-dir --user -r /requirements.txt