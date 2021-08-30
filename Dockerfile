FROM python:3.6-alpine

ADD app.py .
ADD requirements.txt .
ADD dependencies ./dependencies

ENV PACKAGES=org.apache.spark:spark-avro_2.12:3.1.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2

RUN apk add zip
RUN python -m venv venv
RUN source venv/bin/activate
RUN pip install -U pip
RUN pip install -r requirements.txt
RUN zip -rq dependencies.zip dependencies/

CMD $SPARK_HOME/bin/spark-submit \
    --name "Sample Spark Application" \
    --packages $PACKAGES \
    --py-files "dependencies.zip" \
    app.py