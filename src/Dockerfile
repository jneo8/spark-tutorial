FROM ubuntu

RUN apt update
RUN apt install -y default-jre
RUN apt install -y python3-dev python3 curl
RUN curl https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py
RUN python3 /tmp/get-pip.py
RUN python3 -V

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

COPY . /app
WORKDIR /app
RUN pip3 install -r requirements.txt

CMD ["python3", "/app/app.py"]
