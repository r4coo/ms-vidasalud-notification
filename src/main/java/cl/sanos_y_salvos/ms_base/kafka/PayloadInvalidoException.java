package cl.sanos_y_salvos.ms_base.kafka;

/** Mensaje mal formado o incompleto: reintentar no lo arregla, por eso no se reintenta. */
public class PayloadInvalidoException extends RuntimeException {
    public PayloadInvalidoException(String message, Throwable cause) { super(message, cause); }
    public PayloadInvalidoException(String message) { super(message); }
}
