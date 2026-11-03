package cl.sanos_y_salvos.ms_base.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cl.sanos_y_salvos.ms_base.api.dto.AtencionEvent;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class NotificacionListenerTest {
    private AtencionEvent ev(String tipo, String estado, String motivo) {
        return new AtencionEvent(UUID.randomUUID(), tipo, estado, motivo, "p@vidasalud.cl", "Medicina general",
                LocalDate.of(2026, 11, 3), LocalTime.of(10, 0));
    }

    @Test
    void confirmada() {
        assertEquals("Atención confirmada", NotificacionListener.asunto(ev("CONFIRMADA", "CONFIRMADA", null)));
    }

    @Test
    void rechazadaIncluyeMotivo() {
        assertTrue(NotificacionListener.cuerpo(ev("RECHAZADA", "SOLICITADA", "Sin cupos")).contains("Sin cupos"));
    }

    @Test
    void cambioDeEstado() {
        assertEquals("Tu atención está en espera", NotificacionListener.asunto(ev("ESTADO", "EN_ESPERA", null)));
    }
}
