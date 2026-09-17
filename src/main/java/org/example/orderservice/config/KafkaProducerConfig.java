package org.example.orderservice.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.orderservice.event.OrderCreatedEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Cấu hình Kafka Producer.
 *
 * =====================================================================
 * BUG-03 — Tại sao phải dùng orderId làm Kafka Key?
 * =====================================================================
 * Kafka phân phối message vào partition dựa trên Key:
 *   partition = hash(key) % numPartitions
 *
 * Nếu KHÔNG truyền Key:
 *   kafkaTemplate.send(topic, event);   ← WRONG
 *   → Kafka dùng round-robin → các event của cùng 1 đơn hàng
 *     (order.created, order.paid, order.shipped, order.completed)
 *     có thể rơi vào các partition KHÁC NHAU
 *   → Consumer nhận sai thứ tự → nghiệp vụ sai (vd: ship trước khi paid)
 *
 * Nếu DÙNG orderId làm Key:
 *   kafkaTemplate.send(topic, orderId, event);  ← CORRECT
 *   → Tất cả event của ORD-001 luôn vào cùng 1 partition
 *   → Consumer xử lý đúng thứ tự: created → paid → shipped → completed
 * =====================================================================
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * Producer factory với:
     *   Key   → StringSerializer  (orderId là String)
     *   Value → JsonSerializer    (OrderCreatedEvent serialize → JSON)
     */
    @Bean
    public ProducerFactory<String, OrderCreatedEvent> orderEventProducerFactory() {
        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Key serializer: orderId là String
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Value serializer: serialize OrderCreatedEvent thành JSON bytes
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // Không gửi type header trong message (consumer dễ deserialize hơn)
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        // Đảm bảo message không bị mất khi broker gặp sự cố
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // Retry 3 lần nếu send thất bại tạm thời (network glitch)
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * KafkaTemplate dùng trong OrderEventProducer.
     * Generic type <String, OrderCreatedEvent>:
     *   String           = Key type   → orderId
     *   OrderCreatedEvent = Value type → event payload
     */
    @Bean
    public KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate() {
        return new KafkaTemplate<>(orderEventProducerFactory());
    }

    /**
     * Tạo topic "storex-order-events" với 5 partitions nếu chưa tồn tại.
     * Spring Kafka tự động tạo topic này khi ứng dụng khởi động
     * (yêu cầu Kafka broker đang chạy).
     *
     * 5 partitions → cho phép tối đa 5 consumer song song xử lý event.
     */
    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name("storex-order-events")
                .partitions(5)
                .replicas(1)   // 1 replica cho môi trường local/dev
                .build();
    }
}
