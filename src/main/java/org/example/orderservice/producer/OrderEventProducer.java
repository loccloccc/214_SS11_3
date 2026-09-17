package org.example.orderservice.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.orderservice.event.OrderCreatedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.CompletableFuture;

/**
 * Component chịu trách nhiệm publish OrderCreatedEvent lên Kafka.
 *
 * WebFlux Integration:
 *   KafkaTemplate.send() trả về CompletableFuture (blocking-ish).
 *   Chúng ta wrap nó thành Mono để tích hợp với reactive pipeline
 *   mà không cần gọi .block().
 *
 *   Mono.fromFuture() chuyển CompletableFuture → Mono mà không block
 *   event loop của Netty/WebFlux.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventProducer {

    static final String TOPIC = "storex-order-events";

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    /**
     * Publish event "order.created" lên Kafka.
     *
     * =====================================================================
     * BUG-03 FIX — Sử dụng orderId làm Kafka Key
     * =====================================================================
     * kafkaTemplate.send(TOPIC, orderId, event)
     *                              ↑
     *                         Kafka Key = orderId
     *
     * Điều này đảm bảo tất cả event của cùng một đơn hàng
     * (order.created, order.paid, order.shipped, …) đều được route
     * vào CÙNG PARTITION → consumer xử lý đúng thứ tự.
     * =====================================================================
     *
     * @param event OrderCreatedEvent cần publish
     * @return Mono<Void> — hoàn thành khi Kafka xác nhận gửi thành công,
     *         hoặc onError nếu gửi thất bại
     */
    public Mono<Void> publishOrderCreatedEvent(OrderCreatedEvent event) {
        String orderId = event.getOrderId();

        log.info("Publishing order.created event | Kafka topic={} | Kafka key={}",
                TOPIC, orderId);

        // BUG-03: BẮT BUỘC truyền orderId làm Key (tham số thứ 2)
        // KHÔNG ĐƯỢC viết: kafkaTemplate.send(TOPIC, event)
        CompletableFuture<SendResult<String, OrderCreatedEvent>> future =
                kafkaTemplate.send(TOPIC, orderId, event);

        // Chuyển CompletableFuture → Mono để không block WebFlux event loop
        return Mono.fromFuture(future)
                .doOnSuccess(result -> {
                    var metadata = result.getRecordMetadata();
                    log.info("Successfully published order.created event | orderId={} | partition={} | offset={}",
                            orderId, metadata.partition(), metadata.offset());
                })
                .doOnError(ex ->
                    // Error handling: log rõ ràng, không nuốt exception
                    log.error("Failed to publish order.created event | orderId={} | error={}",
                            orderId, ex.getMessage(), ex)
                )
                // Chỉ propagate error, không cần return value
                .then();
    }
}
