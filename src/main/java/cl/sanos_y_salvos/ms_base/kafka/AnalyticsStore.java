package cl.sanos_y_salvos.ms_base.kafka;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

/** Estado en memoria que el grupo de analítica va armando a partir de los eventos (demostración en tiempo real). */
@Component
public class AnalyticsStore {
    private static final int MAX_ALERTAS = 20;
    private static final int MAX_IDS = 5000;

    public record Resumen(long eventosAtenciones, long eventosCupos, long duplicadosDescartados,
                          Map<String, Long> porTipo, Map<String, Long> porPrestacion,
                          Map<String, Integer> cuposDisponibles, List<String> alertasRecientes,
                          Instant ultimaActividad) {}

    private final AtomicLong atenciones = new AtomicLong();
    private final AtomicLong cupos = new AtomicLong();
    private final AtomicLong duplicados = new AtomicLong();
    private final Map<String, AtomicLong> porTipo = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> porPrestacion = new ConcurrentHashMap<>();
    private final Map<String, Integer> disponibles = new ConcurrentHashMap<>();
    private final Set<String> vistos = new LinkedHashSet<>();
    private final Deque<String> alertas = new ArrayDeque<>();
    private volatile Instant ultima;

    /** @return false si el evento ya se había procesado (reentrega). */
    public synchronized boolean registrarAtencion(AtencionKafkaEvent e) {
        if (!marcarVisto(e.eventId())) return false;
        atenciones.incrementAndGet();
        porTipo.computeIfAbsent(e.tipo(), k -> new AtomicLong()).incrementAndGet();
        if (e.prestacion() != null) {
            porPrestacion.computeIfAbsent(e.prestacion(), k -> new AtomicLong()).incrementAndGet();
        }
        ultima = Instant.now();
        return true;
    }

    public synchronized boolean registrarCupo(CupoOcupacionEvent e) {
        if (!marcarVisto(e.eventId())) return false;
        cupos.incrementAndGet();
        disponibles.put(e.prestacionId() + "|" + e.fecha() + "|" + e.hora(), e.disponibles());
        ultima = Instant.now();
        return true;
    }

    public synchronized void registrarAlerta(String texto) {
        alertas.addFirst(texto);
        while (alertas.size() > MAX_ALERTAS) alertas.removeLast();
    }

    public synchronized Resumen resumen() {
        return new Resumen(atenciones.get(), cupos.get(), duplicados.get(), copiar(porTipo), copiar(porPrestacion),
                new TreeMap<>(disponibles), List.copyOf(alertas), ultima);
    }

    private boolean marcarVisto(String eventId) {
        if (eventId == null) return true;
        if (!vistos.add(eventId)) {
            duplicados.incrementAndGet();
            return false;
        }
        if (vistos.size() > MAX_IDS) {
            var it = vistos.iterator();
            it.next();
            it.remove();
        }
        return true;
    }

    private static Map<String, Long> copiar(Map<String, AtomicLong> origen) {
        Map<String, Long> out = new TreeMap<>();
        origen.forEach((k, v) -> out.put(k, v.get()));
        return out;
    }
}
