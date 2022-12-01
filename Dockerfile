FROM ubuntu as build

WORKDIR /workspace/app

RUN apt-get update 
RUN apt-get install -y openjdk-17-jdk
RUN apt-get install -y maven
RUN apt-get install -y git

COPY mvnw .
COPY .mvn .mvn
COPY pom.xml .

WORKDIR /workspace/app1
COPY checkout_commons_version .
COPY pom.xml .
RUN ./checkout_commons_version
WORKDIR /workspace/app


RUN ./mvnw dependency:copy-dependencies
# RUN ./mvnw dependency:go-offline

COPY src src
# COPY api-spec api-spec
RUN ./mvnw install -DskipTests # --offline
RUN mkdir target/extracted && java -Djarmode=layertools -jar target/*.jar extract --destination target/extracted

FROM openjdk:17-slim

RUN addgroup --system user && adduser --ingroup user --system user
USER user:user

WORKDIR /app/

ARG EXTRACTED=/workspace/app/target/extracted

ADD --chown=user https://github.com/microsoft/ApplicationInsights-Java/releases/download/3.4.1/applicationinsights-agent-3.4.1.jar ./applicationinsights-agent.jar
COPY --chown=user applicationinsights.json ./applicationinsights.json

COPY --from=build --chown=user ${EXTRACTED}/dependencies/ ./
RUN true
COPY --from=build --chown=user ${EXTRACTED}/spring-boot-loader/ ./
RUN true
COPY --from=build --chown=user ${EXTRACTED}/snapshot-dependencies/ ./
RUN true
COPY --from=build --chown=user ${EXTRACTED}/application/ ./
RUN true


ENTRYPOINT ["java","-javaagent:applicationinsights-agent.jar","--enable-preview","org.springframework.boot.loader.JarLauncher"]
