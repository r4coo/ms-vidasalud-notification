package cl.sanos_y_salvos.ms_base.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Grupo vidasalud-alertas: reacciona en tiempo real a los eventos de atenciones.
 * Es un grupo distinto del de analítica, así que recibe su propia copia de cada mensaje.
 */
@Component
public class AlertasKafkaListener {
    private static final Logger log = LoggerFactory.getLogger(AlertasKafkaListener.class);

    private final EventoParser parser;
    private final AnalyticsStore store;

    public AlertasKafkaListener(EventoParser parser, AnalyticsStore store) {
        this.parser = parser;
        this.store = store;
    }

    @KafkaListener(topics = "${app.kafka.topic.atenciones-eventos}", groupId = "${app.kafka.group.alertas}",
            containerFactory = "kafkaListenerContainerFactory")
    public void onAtencion(String payload) {
        AtencionKafkaEvent e = parser.atencion(payload);
        String alerta = generarAlerta(e);
        if (alerta != null) {
            log.warn("[alertas] {}", alerta);
            store.registrarAlerta(alerta);
        }
    }

    /** Devuelve el texto de la alerta, o null si el evento no la requiere. */
    static String generarAlerta(AtencionKafkaEvent e) {
        if ("RECHAZADA".equals(e.tipo())) {
            return "Atención " + e.atencionId() + " rechazada (" + e.prestacion() + "): " + e.motivo();
        }
        if ("CANCELADA".equals(e.estado())) {
            return "Atención " + e.atencionId() + " cancelada (" + e.prestacion() + " " + e.fecha() + " " + e.hora() + ")";
        }
        return null;
    }
}
