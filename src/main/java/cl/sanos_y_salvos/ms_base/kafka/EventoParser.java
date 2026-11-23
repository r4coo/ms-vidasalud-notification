package cl.sanos_y_salvos.ms_base.kafka;

import java.util.Set;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/** Convierte el JSON recibido en DTO y valida los campos obligatorios. */
@Component
public class EventoParser {
    private static final Set<String> TIPOS = Set.of("ESTADO", "CONFIRMADA", "RECHAZADA");

    private final JsonMapper mapper = JsonMapper.builder().build();

    public AtencionKafkaEvent atencion(String json) {
        AtencionKafkaEvent e = leer(json, AtencionKafkaEvent.class);
        if (e.atencionId() == null) throw new PayloadInvalidoException("Evento sin atencionId");
        if (e.tipo() == null || !TIPOS.contains(e.tipo())) {
            throw new PayloadInvalidoException("Tipo de evento no válido: " + e.tipo());
        }
        if (e.estado() == null || e.estado().isBlank()) throw new PayloadInvalidoException("Evento sin estado");
        return e;
    }

    public CupoOcupacionEvent cupo(String json) {
        CupoOcupacionEvent e = leer(json, CupoOcupacionEvent.class);
        if (e.prestacionId() == null || e.fecha() == null || e.hora() == null) {
            throw new PayloadInvalidoException("Evento de cupo incompleto");
        }
        if (e.disponibles() < 0) throw new PayloadInvalidoException("Cupos disponibles negativos");
        return e;
    }

    private <T> T leer(String json, Class<T> tipo) {
        if (json == null || json.isBlank()) throw new PayloadInvalidoException("Mensaje vacío");
        try {
            T valor = mapper.readValue(json, tipo);
            if (valor == null) throw new PayloadInvalidoException("Mensaje vacío");
            return valor;
        } catch (PayloadInvalidoException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new PayloadInvalidoException("JSON inválido: " + e.getMessage(), e);
        }
    }
}
