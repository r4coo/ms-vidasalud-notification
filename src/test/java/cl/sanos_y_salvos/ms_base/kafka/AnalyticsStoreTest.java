package cl.sanos_y_salvos.ms_base.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class AnalyticsStoreTest {
    private final AnalyticsStore store = new AnalyticsStore();

    private AtencionKafkaEvent atencion(String eventId, String tipo, String prestacion) {
        return new AtencionKafkaEvent(eventId, 1, tipo, UUID.randomUUID(), "CONFIRMADA", null, "p@vidasalud.cl",
                prestacion, LocalDate.of(2026, 11, 3), LocalTime.of(10, 0), null);
    }

    @Test
    void cuentaEventosPorTipoYPrestacion() {
        store.registrarAtencion(atencion("a", "CONFIRMADA", "Kinesiología"));
        store.registrarAtencion(atencion("b", "CONFIRMADA", "Kinesiología"));
        store.registrarAtencion(atencion("c", "RECHAZADA", "Medicina general"));

        AnalyticsStore.Resumen r = store.resumen();
        assertEquals(3, r.eventosAtenciones());
        assertEquals(2L, r.porTipo().get("CONFIRMADA"));
        assertEquals(1L, r.porTipo().get("RECHAZADA"));
        assertEquals(2L, r.porPrestacion().get("Kinesiología"));
        assertNotNull(r.ultimaActividad());
    }

    @Test
    void descartaEventosDuplicados() {
        assertTrue(store.registrarAtencion(atencion("dup", "ESTADO", "Kinesiología")));
        assertFalse(store.registrarAtencion(atencion("dup", "ESTADO", "Kinesiología")));
        AnalyticsStore.Resumen r = store.resumen();
        assertEquals(1, r.eventosAtenciones());
        assertEquals(1, r.duplicadosDescartados());
    }

    @Test
    void guardaElUltimoValorDeCuposPorHorario() {
        store.registrarCupo(new CupoOcupacionEvent("c1", 1, 2L, "Kinesiología", LocalDate.of(2026, 11, 3),
                LocalTime.of(10, 0), 3, null));
        store.registrarCupo(new CupoOcupacionEvent("c2", 1, 2L, "Kinesiología", LocalDate.of(2026, 11, 3),
                LocalTime.of(10, 0), 2, null));
        assertEquals(2, store.resumen().cuposDisponibles().get("2|2026-11-03|10:00"));
        assertEquals(2, store.resumen().eventosCupos());
    }

    @Test
    void limitaLasAlertasRecientes() {
        for (int i = 0; i < 30; i++) store.registrarAlerta("alerta " + i);
        var alertas = store.resumen().alertasRecientes();
        assertEquals(20, alertas.size());
        assertEquals("alerta 29", alertas.get(0));
    }
}
