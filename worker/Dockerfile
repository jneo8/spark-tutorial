FROM bde2020/spark-worker:2.4.0-hadoop2.7
RUN apk add --update alpine-sdk python3-dev
COPY requirements.txt /tmp
RUN pip3 install -r /tmp/requirements.txt
