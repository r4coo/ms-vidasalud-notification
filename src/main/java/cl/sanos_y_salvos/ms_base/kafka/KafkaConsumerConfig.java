package cl.sanos_y_salvos.ms_base.kafka;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Cliente consumidor Kafka: offsets confirmados por el contenedor tras procesar cada mensaje,
 * reintentos acotados ante fallos transitorios y salto del mensaje si es inválido o agota los reintentos.
 */
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, String> kafkaConsumerFactory(
            @Value("${app.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);          // el commit lo hace el contenedor (RECORD)
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");      // un grupo nuevo lee desde el principio
        cfg.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        cfg.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15000);
        cfg.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        return new DefaultKafkaConsumerFactory<>(cfg, new StringDeserializer(), new StringDeserializer());
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(@Value("${app.kafka.consumer.retry-interval-ms}") long intervalo,
                                                 @Value("${app.kafka.consumer.retry-attempts}") long intentos) {
        DefaultErrorHandler handler = new DefaultErrorHandler(new FixedBackOff(intervalo, intentos));
        handler.addNotRetryableExceptions(PayloadInvalidoException.class);   // un JSON roto no se arregla reintentando
        return handler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> kafkaConsumerFactory, DefaultErrorHandler kafkaErrorHandler,
            @Value("${app.kafka.consumer.concurrency}") int concurrencia) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(kafkaConsumerFactory);
        factory.setConcurrency(concurrencia);                              // hasta 3 hilos: uno por partición
        factory.setCommonErrorHandler(kafkaErrorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }
}
