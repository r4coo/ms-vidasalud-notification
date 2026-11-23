package cl.sanos_y_salvos.ms_base.kafka;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Métricas construidas en tiempo real a partir de los eventos de Kafka. */
@RestController
@RequestMapping("/api/analytics")
public class AnalyticsController {
    private final AnalyticsStore store;

    public AnalyticsController(AnalyticsStore store) {
        this.store = store;
    }

    @GetMapping("/resumen")
    public AnalyticsStore.Resumen resumen() {
        return store.resumen();
    }
}
