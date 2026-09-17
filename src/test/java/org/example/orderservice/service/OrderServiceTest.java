package org.example.orderservice.service;

import org.example.orderservice.dto.CreateOrderRequest;
import org.example.orderservice.dto.OrderResponse;
import org.example.orderservice.event.OrderCreatedEvent;
import org.example.orderservice.producer.OrderEventProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test cho OrderService:
 *   - orderId được tạo với prefix ORD-
 *   - OrderCreatedEvent có eventType = "order.created"
 *   - Response chứa orderId
 */
@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock
    private OrderEventProducer orderEventProducer;

    @Captor
    private ArgumentCaptor<OrderCreatedEvent> eventCaptor;

    private OrderService orderService;

    @BeforeEach
    void setUp() {
        orderService = new OrderService(orderEventProducer);
    }

    @Test
    @DisplayName("createOrder → tạo orderId có prefix ORD- và trả OrderResponse")
    void createOrder_shouldGenerateOrderIdAndReturnResponse() {
        // Arrange
        CreateOrderRequest request = new CreateOrderRequest(
                "CUS-001", "PROD-001", 2, new BigDecimal("150000")
        );
        when(orderEventProducer.publishOrderCreatedEvent(any())).thenReturn(Mono.empty());

        // Act & Assert
        StepVerifier.create(orderService.createOrder(request))
                .assertNext(response -> {
                    assertThat(response).isNotNull();
                    assertThat(response.getOrderId()).startsWith("ORD-");
                    assertThat(response.getOrderId()).hasSize(12); // ORD- + 8 chars
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("createOrder → publish event với eventType=order.created và đúng orderId")
    void createOrder_shouldPublishEventWithCorrectData() {
        // Arrange
        CreateOrderRequest request = new CreateOrderRequest(
                "CUS-002", "PROD-002", 3, new BigDecimal("300000")
        );
        when(orderEventProducer.publishOrderCreatedEvent(any())).thenReturn(Mono.empty());

        // Act
        StepVerifier.create(orderService.createOrder(request))
                .assertNext(response -> assertThat(response.getOrderId()).startsWith("ORD-"))
                .verifyComplete();

        // Assert event được publish
        verify(orderEventProducer).publishOrderCreatedEvent(eventCaptor.capture());
        OrderCreatedEvent capturedEvent = eventCaptor.getValue();

        assertThat(capturedEvent.getEventType()).isEqualTo("order.created");
        assertThat(capturedEvent.getOrderId()).startsWith("ORD-");
        assertThat(capturedEvent.getCustomerId()).isEqualTo("CUS-002");
        assertThat(capturedEvent.getProductId()).isEqualTo("PROD-002");
        assertThat(capturedEvent.getQuantity()).isEqualTo(3);
    }

    @Test
    @DisplayName("createOrder → vẫn trả 202 dù Kafka publish thất bại")
    void createOrder_whenKafkaFails_shouldStillReturnResponse() {
        // Arrange
        CreateOrderRequest request = new CreateOrderRequest(
                "CUS-003", "PROD-003", 1, new BigDecimal("100000")
        );
        when(orderEventProducer.publishOrderCreatedEvent(any()))
                .thenReturn(Mono.error(new RuntimeException("Kafka down")));

        // Act & Assert — vẫn nhận được response (không throw exception)
        StepVerifier.create(orderService.createOrder(request))
                .assertNext(response -> {
                    assertThat(response).isInstanceOf(OrderResponse.class);
                    assertThat(response.getOrderId()).startsWith("ORD-");
                })
                .verifyComplete();
    }
}
