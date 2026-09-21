FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn -B -q package -DskipTests

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /build/target/Broker-1.0-SNAPSHOT.jar ./broker.jar
EXPOSE 5000
ENTRYPOINT ["java", "-jar", "broker.jar"]
