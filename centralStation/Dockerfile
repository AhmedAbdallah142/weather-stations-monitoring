FROM python:3.9-slim-buster

COPY . /app

WORKDIR /app/centralStation

# # Install java
RUN apt-get update && apt-get install -y openjdk-11-jdk

# RUN sed -i 's/\r$//' mvnw && chmod +x mvnw && ./mvnw clean install

WORKDIR /app

# Install python dependencies
RUN pip3 install pandas elasticsearch pyspark watchdog

EXPOSE 8082

# ENTRYPOINT ["sh", "-c", "python -u elasticSearch/elasticSearch.py && java -jar centralStation/target/central-station.jar"]
ENTRYPOINT [ "python", "-u", "elasticSearch/counter.py"]
