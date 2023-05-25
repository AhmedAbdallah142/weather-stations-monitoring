FROM python:3.9-slim-buster

# Install java
RUN apt-get update 
RUN apt-get install -y openjdk-11-jdk-headless
# Install python dependencies
RUN pip3 install pandas elasticsearch pyspark watchdog
# Install supervisord
RUN apt-get install -y supervisor

# Copy the supervisord configuration file
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

COPY . /app

WORKDIR /app/centralStation

# RUN sed -i 's/\r$//' mvnw && chmod +x mvnw && ./mvnw clean install

WORKDIR /app

EXPOSE 8082

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
