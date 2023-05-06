FROM openjdk:17-alpine
COPY . /app
WORKDIR /app

RUN sed -i 's/\r$//' mvnw && chmod +x mvnw && ./mvnw clean install

ENTRYPOINT [ "java", "-jar", "target/central-station-jar-with-dependencies.jar"]