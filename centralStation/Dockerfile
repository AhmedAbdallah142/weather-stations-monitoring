FROM openjdk:17-alpine

COPY . /app
WORKDIR /app

# Install Python and required dependencies
RUN apk add --no-cache python3 && \
    pip3 install --upgrade pip && \
    pip3 install pandas elasticsearch pyspark watchdog

WORKDIR /app/centralStation

RUN sed -i 's/\r$//' mvnw && chmod +x mvnw && ./mvnw clean install

WORKDIR /app

EXPOSE 8082

ENTRYPOINT ["sh", "-c", "python elasticSearch/elasticSearch.py && java -jar centralStation/target/central-station.jar"]
