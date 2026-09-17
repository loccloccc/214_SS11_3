package org.example.orderservice.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.example.orderservice.event.OrderCreatedEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test 2: Kafka Producer
 *   - Gọi với đúng topic = "storex-order-events"
 *   - BUG-03: Kafka Key = orderId (KHÔNG để null)
 */
@ExtendWith(MockitoExtension.class)
class OrderEventProducerTest {

    @Mock
    private KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    @Captor
    private ArgumentCaptor<String> topicCaptor;

    @Captor
    private ArgumentCaptor<String> keyCaptor;

    @Captor
    private ArgumentCaptor<OrderCreatedEvent> eventCaptor;

    private OrderEventProducer producer;

    @BeforeEach
    void setUp() {
        producer = new OrderEventProducer(kafkaTemplate);
    }

    @Test
    @DisplayName("publishOrderCreatedEvent → gửi đúng topic và dùng orderId làm Kafka Key (BUG-03)")
    void publishOrderCreatedEvent_shouldUseOrderIdAsKafkaKey() {
        // Arrange
        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .eventType("order.created")
                .orderId("ORD-TESTKEY1")
                .customerId("CUS-001")
                .productId("PROD-001")
                .quantity(2)
                .totalAmount(new BigDecimal("150000"))
                .build();

        // Mock SendResult
        ProducerRecord<String, OrderCreatedEvent> record =
                new ProducerRecord<>(OrderEventProducer.TOPIC, "ORD-TESTKEY1", event);
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition(OrderEventProducer.TOPIC, 2), 0L, 0, 0L, 0, 0);
        SendResult<String, OrderCreatedEvent> sendResult = new SendResult<>(record, metadata);

        CompletableFuture<SendResult<String, OrderCreatedEvent>> future =
                CompletableFuture.completedFuture(sendResult);

        when(kafkaTemplate.send(
                eq(OrderEventProducer.TOPIC),
                eq("ORD-TESTKEY1"),
                eq(event)
        )).thenReturn(future);

        // Act
        var mono = producer.publishOrderCreatedEvent(event);

        // Assert — reactive pipeline completes successfully
        StepVerifier.create(mono)
                .verifyComplete();

        // Verify: kafkaTemplate.send được gọi với đúng 3 tham số (topic, key, value)
        verify(kafkaTemplate).send(topicCaptor.capture(), keyCaptor.capture(), eventCaptor.capture());

        // Topic phải là storex-order-events
        assertThat(topicCaptor.getValue()).isEqualTo("storex-order-events");

        // BUG-03: KEY phải là orderId, KHÔNG được null hoặc empty
        assertThat(keyCaptor.getValue())
                .as("Kafka Key phải là orderId (BUG-03)")
                .isNotNull()
                .isNotEmpty()
                .isEqualTo("ORD-TESTKEY1");

        // Value phải là event đúng
        assertThat(eventCaptor.getValue().getEventType()).isEqualTo("order.created");
        assertThat(eventCaptor.getValue().getOrderId()).isEqualTo("ORD-TESTKEY1");
    }

    @Test
    @DisplayName("publishOrderCreatedEvent → Mono onError khi Kafka send thất bại")
    void publishOrderCreatedEvent_whenKafkaFails_shouldReturnMonoError() {
        // Arrange
        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .eventType("order.created")
                .orderId("ORD-FAILTEST")
                .customerId("CUS-001")
                .productId("PROD-001")
                .quantity(1)
                .totalAmount(new BigDecimal("50000"))
                .build();

        CompletableFuture<SendResult<String, OrderCreatedEvent>> failedFuture =
                CompletableFuture.failedFuture(new RuntimeException("Kafka broker unavailable"));

        when(kafkaTemplate.send(
                eq(OrderEventProducer.TOPIC),
                eq("ORD-FAILTEST"),
                eq(event)
        )).thenReturn(failedFuture);

        // Act & Assert — Mono phải emit error
        StepVerifier.create(producer.publishOrderCreatedEvent(event))
                .expectErrorMatches(ex -> ex.getMessage().contains("Kafka broker unavailable"))
                .verify();
    }
}
