package cl.sanos_y_salvos.ms_base.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class AnalyticsKafkaListenerTest {
    private final AnalyticsStore store = new AnalyticsStore();
    private final AnalyticsKafkaListener listener = new AnalyticsKafkaListener(new EventoParser(), store);

    @Test
    void procesaAtencionYCupo_yDescartaElDuplicado() {
        String atencion = "{\"eventId\":\"e1\",\"tipo\":\"CONFIRMADA\",\"estado\":\"CONFIRMADA\",\"atencionId\":\""
                + UUID.randomUUID() + "\",\"prestacion\":\"Kinesiología\"}";
        listener.onAtencion(atencion);
        listener.onAtencion(atencion);
        listener.onCupo("{\"eventId\":\"c1\",\"prestacionId\":2,\"prestacion\":\"Kinesiología\","
                + "\"fecha\":\"2026-11-03\",\"hora\":\"10:00:00\",\"disponibles\":1}");

        AnalyticsStore.Resumen r = store.resumen();
        assertEquals(1, r.eventosAtenciones());
        assertEquals(1, r.duplicadosDescartados());
        assertEquals(1, r.eventosCupos());
    }

    @Test
    void cupoInvalido_lanzaPayloadInvalido() {
        assertThrows(PayloadInvalidoException.class, () -> listener.onCupo("{\"prestacionId\":2}"));
    }
}
