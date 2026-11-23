package cl.sanos_y_salvos.ms_base.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Grupo vidasalud-analitica: arma las métricas en tiempo real leyendo los dos tópicos. */
@Component
public class AnalyticsKafkaListener {
    private static final Logger log = LoggerFactory.getLogger(AnalyticsKafkaListener.class);

    private final EventoParser parser;
    private final AnalyticsStore store;

    public AnalyticsKafkaListener(EventoParser parser, AnalyticsStore store) {
        this.parser = parser;
        this.store = store;
    }

    @KafkaListener(topics = "${app.kafka.topic.atenciones-eventos}", groupId = "${app.kafka.group.analitica}",
            containerFactory = "kafkaListenerContainerFactory")
    public void onAtencion(String payload) {
        AtencionKafkaEvent e = parser.atencion(payload);
        if (store.registrarAtencion(e)) {
            log.info("[analítica] atención={} tipo={} estado={}", e.atencionId(), e.tipo(), e.estado());
        } else {
            log.debug("[analítica] evento duplicado {} descartado", e.eventId());
        }
    }

    @KafkaListener(topics = "${app.kafka.topic.catalogo-cupos}", groupId = "${app.kafka.group.analitica}",
            containerFactory = "kafkaListenerContainerFactory")
    public void onCupo(String payload) {
        CupoOcupacionEvent e = parser.cupo(payload);
        if (store.registrarCupo(e)) {
            log.info("[analítica] cupo prestación={} {} {} disponibles={}", e.prestacionId(), e.fecha(), e.hora(),
                    e.disponibles());
        }
    }
}
