package cl.sanos_y_salvos.ms_base.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class EventoParserTest {
    private final EventoParser parser = new EventoParser();
    private final UUID id = UUID.randomUUID();

    @Test
    void atencionValida_seConvierteEnDto() {
        String json = "{\"eventId\":\"e1\",\"version\":1,\"tipo\":\"CONFIRMADA\",\"atencionId\":\"" + id
                + "\",\"estado\":\"CONFIRMADA\",\"paciente\":\"p@vidasalud.cl\",\"prestacion\":\"Kinesiología\","
                + "\"fecha\":\"2026-11-03\",\"hora\":\"10:00:00\",\"ocurridoEn\":\"2026-11-02T12:00:00Z\"}";
        AtencionKafkaEvent e = parser.atencion(json);
        assertEquals(id, e.atencionId());
        assertEquals("CONFIRMADA", e.tipo());
        assertEquals("Kinesiología", e.prestacion());
    }

    @Test
    void jsonRoto_esInvalido() {
        assertThrows(PayloadInvalidoException.class, () -> parser.atencion("{no es json"));
    }

    @Test
    void mensajeVacio_esInvalido() {
        assertThrows(PayloadInvalidoException.class, () -> parser.atencion(""));
        assertThrows(PayloadInvalidoException.class, () -> parser.atencion(null));
    }

    @Test
    void atencionSinId_esInvalida() {
        assertThrows(PayloadInvalidoException.class,
                () -> parser.atencion("{\"tipo\":\"ESTADO\",\"estado\":\"SOLICITADA\"}"));
    }

    @Test
    void tipoDesconocido_esInvalido() {
        assertThrows(PayloadInvalidoException.class, () -> parser.atencion(
                "{\"tipo\":\"OTRO\",\"estado\":\"SOLICITADA\",\"atencionId\":\"" + id + "\"}"));
    }

    @Test
    void cupoValido_seConvierteEnDto() {
        CupoOcupacionEvent e = parser.cupo("{\"eventId\":\"c1\",\"version\":1,\"prestacionId\":2,"
                + "\"prestacion\":\"Kinesiología\",\"fecha\":\"2026-11-03\",\"hora\":\"10:00:00\",\"disponibles\":3}");
        assertEquals(2L, e.prestacionId());
        assertEquals(3, e.disponibles());
    }

    @Test
    void cupoIncompletoONegativo_esInvalido() {
        assertThrows(PayloadInvalidoException.class, () -> parser.cupo("{\"prestacionId\":2}"));
        assertThrows(PayloadInvalidoException.class, () -> parser.cupo(
                "{\"prestacionId\":2,\"fecha\":\"2026-11-03\",\"hora\":\"10:00:00\",\"disponibles\":-1}"));
    }
}
