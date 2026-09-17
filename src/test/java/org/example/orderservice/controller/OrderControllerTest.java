package org.example.orderservice.controller;

import org.example.orderservice.dto.CreateOrderRequest;
import org.example.orderservice.dto.OrderResponse;
import org.example.orderservice.service.OrderService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Test 1: POST /api/v1/orders
 *   - Trả HTTP 202 Accepted
 *   - Response body chứa orderId
 */
@WebFluxTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockBean
    private OrderService orderService;

    @Test
    @DisplayName("POST /api/v1/orders → 202 Accepted với orderId trong response")
    void createOrder_shouldReturn202WithOrderId() {
        // Arrange
        CreateOrderRequest request = new CreateOrderRequest(
                "CUS-001", "PROD-001", 2, new BigDecimal("150000")
        );

        OrderResponse mockResponse = new OrderResponse("ORD-TESTABCD");
        when(orderService.createOrder(any(CreateOrderRequest.class)))
                .thenReturn(Mono.just(mockResponse));

        // Act & Assert
        webTestClient.post()
                .uri("/api/v1/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.ACCEPTED)               // 202
                .expectBody(OrderResponse.class)
                .value(response -> {
                    assertThat(response).isNotNull();
                    assertThat(response.getOrderId()).isNotBlank();
                    assertThat(response.getOrderId()).isEqualTo("ORD-TESTABCD");
                });
    }

    @Test
    @DisplayName("POST /api/v1/orders với request thiếu field → 400 Bad Request")
    void createOrder_withInvalidRequest_shouldReturn400() {
        // Request thiếu customerId
        String invalidJson = """
                {
                    "productId": "PROD-001",
                    "quantity": 2,
                    "totalAmount": 150000
                }
                """;

        webTestClient.post()
                .uri("/api/v1/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(invalidJson)
                .exchange()
                .expectStatus().isBadRequest();
    }
}
