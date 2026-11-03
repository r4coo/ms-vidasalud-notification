# ms-vidasalud-notification

Consume `q.notificaciones` (`atencion.*`) y registra/envía avisos al paciente.

- Puerto: **8085**
- Stack: Spring Boot 4, Java 25, Gradle.
- Base de datos: PostgreSQL (db_notifications).

## Ejecutar
```bash
./gradlew test
./gradlew bootRun     # requiere RabbitMQ/PostgreSQL (ver repo de infraestructura: vidasalud-infra)
docker build -t ms-vidasalud-notification .
```
Variables de entorno: ver `src/main/resources/application.properties` y el `compose.yaml` del repo de infraestructura.
No subir `.env` ni credenciales.
