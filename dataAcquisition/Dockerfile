FROM openjdk:17-alpine
COPY . /app
WORKDIR /app

RUN sed -i 's/\r$//' mvnw && chmod +x mvnw && ./mvnw clean install

EXPOSE 8080

ENTRYPOINT [ "java", "-jar", "target/data-acquisition-jar-with-dependencies.jar"]