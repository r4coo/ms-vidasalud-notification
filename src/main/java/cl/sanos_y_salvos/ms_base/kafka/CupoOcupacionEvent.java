package cl.sanos_y_salvos.ms_base.kafka;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

/** Evento del tópico compactado vidasalud.catalogo.cupos (clave = prestacionId|fecha|hora). */
public record CupoOcupacionEvent(String eventId, int version, Long prestacionId, String prestacion,
                                 LocalDate fecha, LocalTime hora, int disponibles, Instant ocurridoEn) {}
