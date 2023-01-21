FROM f278386b0cef68136129f5f58c52445590a417b624d62bca158d4dc926c340df

WORKDIR /app

COPY .mvn/ .mvn
COPY mvnw pom.xml ./
RUN ./mvnw dependency:resolve

COPY src ./src

CMD ["./mvnw", "spring-boot:run"]