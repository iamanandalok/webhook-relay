FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /build
COPY gradlew .
COPY gradle ./gradle
COPY build.gradle settings.gradle ./
RUN ./gradlew --no-daemon dependencies
COPY src ./src
RUN ./gradlew --no-daemon clean bootJar -x test

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /build/build/libs/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
