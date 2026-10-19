FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml ./
COPY services ./services
RUN mvn -B -ntp -pl services/control-plane-service -am package -DskipTests

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /workspace/services/control-plane-service/target/*.jar app.jar
EXPOSE 8080 8081 8082 8083
ENTRYPOINT ["java","-jar","/app/app.jar"]
