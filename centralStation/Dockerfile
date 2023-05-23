FROM openjdk:17-alpine
COPY . /app
WORKDIR /app

RUN sed -i 's/\r$//' mvnw && chmod +x mvnw && ./mvnw clean install

EXPOSE 8082

ENTRYPOINT [ "java", "-jar", "target/central-station.jar"]