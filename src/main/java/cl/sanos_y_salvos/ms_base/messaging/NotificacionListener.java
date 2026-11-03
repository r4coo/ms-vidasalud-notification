package cl.sanos_y_salvos.ms_base.messaging;

import cl.sanos_y_salvos.ms_base.api.dto.AtencionEvent;
import cl.sanos_y_salvos.ms_base.api.dto.NotificationDTO;
import cl.sanos_y_salvos.ms_base.api.service.NotificationService;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

/** Dominio notificaciones: genera el aviso al paciente a partir de los eventos atencion.* (topic). */
@Component
public class NotificacionListener {
    private static final Logger log = LoggerFactory.getLogger(NotificacionListener.class);

    private final NotificationService service;
    private final JsonCodec json;
    private final AckSupport ack;

    public NotificacionListener(NotificationService service, JsonCodec json, AckSupport ack) {
        this.service = service;
        this.json = json;
        this.ack = ack;
    }

    @RabbitListener(queues = RabbitNames.Q_NOTIFICACIONES)
    public void onEvento(Message message, Channel channel) throws IOException {
        try {
            AtencionEvent e = json.fromBytes(message.getBody(), AtencionEvent.class);
            if (e.paciente() == null || e.paciente().isBlank()) {
                throw new IllegalArgumentException("Evento sin destinatario");
            }
            NotificationDTO dto = new NotificationDTO();
            dto.setTo(e.paciente());
            dto.setSubject(asunto(e));
            dto.setBody(cuerpo(e));
            service.createNotification(dto);
            ack.ack(channel, message);
            log.info("Notificación generada para {} (atención {})", e.paciente(), e.atencionId());
        } catch (IllegalArgumentException ex) {          // no recuperable
            ack.rejectToDlq(channel, message, ex.getMessage());
        } catch (DataAccessException ex) {               // recuperable
            ack.retryOrDlq(channel, message, RabbitNames.Q_NOTIFICACIONES, ex);
        } catch (RuntimeException ex) {
            ack.rejectToDlq(channel, message, "error inesperado: " + ex);
        }
    }

    static String asunto(AtencionEvent e) {
        return switch (e.tipo() == null ? "" : e.tipo()) {
            case AtencionEvent.CONFIRMADA -> "Atención confirmada";
            case AtencionEvent.RECHAZADA -> "No pudimos confirmar tu atención";
            default -> "Tu atención está " + (e.estado() == null ? "actualizada" : e.estado().toLowerCase().replace('_', ' '));
        };
    }

    static String cuerpo(AtencionEvent e) {
        String detalle = e.prestacion() + " el " + e.fecha() + " a las " + e.hora();
        return switch (e.tipo() == null ? "" : e.tipo()) {
            case AtencionEvent.CONFIRMADA -> "Tu atención de " + detalle + " fue confirmada.";
            case AtencionEvent.RECHAZADA -> "No se pudo confirmar tu atención de " + detalle + ": " + e.motivo() + ".";
            default -> "Tu atención de " + detalle + " ahora está en estado " + e.estado() + ".";
        };
    }
}
