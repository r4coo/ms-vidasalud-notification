# ms-vidasalud-notification

Consume `q.notificaciones` (`atencion.*`) de RabbitMQ y registra/envía avisos al paciente. Además es el **consumidor de Kafka**: procesa los eventos en streaming para analítica y alertas en tiempo real.

- Puerto: **8085**
- Stack: Spring Boot 4, Java 25, Gradle.
- Base de datos: PostgreSQL (db_notifications).

## Kafka (consumidor)
| Grupo | Tópicos | Qué hace |
|---|---|---|
| `vidasalud-analitica` | `vidasalud.atenciones.eventos`, `vidasalud.catalogo.cupos` | Cuenta eventos por tipo/estado y guarda el último cupo disponible por horario (en memoria) |
| `vidasalud-alertas` | `vidasalud.atenciones.eventos` | Registra alertas ante rechazos y cancelaciones |

- `@KafkaListener` con concurrencia 3 y `CooperativeStickyAssignor`; offsets confirmados por mensaje (`AckMode.RECORD`).
- El JSON se valida con un parser propio: un mensaje inválido lanza `PayloadInvalidoException`, se registra y se omite **sin reintentos**; un fallo transitorio se reintenta 3 veces (1 s) y se omite.
- Eventos duplicados se descartan por `eventId`.
- `GET /api/analytics/resumen` (vía BFF para Admin y Recepcionista): `eventosAtenciones`, `eventosCupos`, conteos por estado y `cuposDisponibles`. Lo consume la pantalla *Kafka en vivo* del front.
- La analítica está en memoria: se reinicia al reiniciar el servicio.

## Ejecutar
```bash
./gradlew test
./gradlew bootRun     # requiere RabbitMQ/PostgreSQL (ver repo de infraestructura: vidasalud-infra)
docker build -t ms-vidasalud-notification .
```
Variables de entorno: ver `src/main/resources/application.properties` y el `compose.yaml` del repo de infraestructura.
No subir `.env` ni credenciales.
