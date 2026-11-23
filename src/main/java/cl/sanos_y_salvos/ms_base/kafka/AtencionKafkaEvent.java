package cl.sanos_y_salvos.ms_base.kafka;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

/** Evento del tópico vidasalud.atenciones.eventos (clave = id de la atención). */
public record AtencionKafkaEvent(String eventId, Integer version, String tipo, UUID atencionId, String estado,
                                 String motivo, String paciente, String prestacion, LocalDate fecha,
                                 LocalTime hora, Instant ocurridoEn) {}
