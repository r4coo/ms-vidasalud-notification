package cl.sanos_y_salvos.ms_base.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class AlertasKafkaListenerTest {
    private final AnalyticsStore store = new AnalyticsStore();
    private final AlertasKafkaListener listener = new AlertasKafkaListener(new EventoParser(), store);
    private final UUID id = UUID.randomUUID();

    private static AtencionKafkaEvent evento(String tipo, String estado, String motivo) {
        return new AtencionKafkaEvent("e", 1, tipo, UUID.randomUUID(), estado, motivo, "p@vidasalud.cl",
                "Kinesiología", LocalDate.of(2026, 11, 3), LocalTime.of(10, 0), null);
    }

    @Test
    void rechazo_generaAlertaConElMotivo() {
        String a = AlertasKafkaListener.generarAlerta(evento("RECHAZADA", "SOLICITADA", "Sin cupos"));
        assertTrue(a.contains("rechazada") && a.contains("Sin cupos"), a);
    }

    @Test
    void cancelacion_generaAlerta() {
        assertTrue(AlertasKafkaListener.generarAlerta(evento("ESTADO", "CANCELADA", null)).contains("cancelada"));
    }

    @Test
    void confirmacionNormal_noGeneraAlerta() {
        assertNull(AlertasKafkaListener.generarAlerta(evento("CONFIRMADA", "CONFIRMADA", null)));
    }

    @Test
    void mensajeRechazado_quedaRegistradoEnElResumen() {
        listener.onAtencion("{\"tipo\":\"RECHAZADA\",\"estado\":\"SOLICITADA\",\"atencionId\":\"" + id
                + "\",\"motivo\":\"Sin cupos\",\"prestacion\":\"Kinesiología\"}");
        assertEquals(1, store.resumen().alertasRecientes().size());
    }

    @Test
    void mensajeInvalido_lanzaPayloadInvalido_paraQueNoSeReintente() {
        assertThrows(PayloadInvalidoException.class, () -> listener.onAtencion("basura"));
    }
}
