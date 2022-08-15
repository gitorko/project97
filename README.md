# Project97

Spring Integration

[https://gitorko.github.io/spring-integration-basics/](https://gitorko.github.io/spring-integration-basics/)

### Version

Check version

```bash
$java --version
openjdk 17.0.3 2022-04-19 LTS
```

### RabbitMQ

Run the docker command to start a rabbitmq instance

```bash
docker run -d --hostname my-rabbit -p 8080:15672 -p 5672:5672 rabbitmq:3-management
```

Login to rabbitmq console [http://localhost:8080](http://localhost:8080)

```
username:guest
password: guest
```

### Postgres DB

```
docker run -p 5432:5432 --name pg-container -e POSTGRES_PASSWORD=password -d postgres:9.6.10
docker ps
docker exec -it pg-container psql -U postgres -W postgres
CREATE USER test WITH PASSWORD 'test@123';
CREATE DATABASE "test-db" WITH OWNER "test" ENCODING UTF8 TEMPLATE template0;
grant all PRIVILEGES ON DATABASE "test-db" to test;

docker stop pg-container
docker start pg-container
```

### Dev

To run the code.

```bash
./gradlew clean build
./gradlew bootRun
```
