package org.example.orderservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.orderservice.dto.CreateOrderRequest;
import org.example.orderservice.dto.OrderResponse;
import org.example.orderservice.event.OrderCreatedEvent;
import org.example.orderservice.producer.OrderEventProducer;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * Business logic layer cho Order.
 *
 * Flow:
 *   1. Nhận CreateOrderRequest
 *   2. Tạo orderId dạng "ORD-xxxxxxxx"
 *   3. Build OrderCreatedEvent
 *   4. Publish event lên Kafka (non-blocking)
 *   5. Trả OrderResponse { orderId }
 *
 * Không có logic trừ kho / trừ tiền ở đây —
 * những nghiệp vụ đó sẽ được xử lý bởi downstream consumers.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderEventProducer orderEventProducer;

    /**
     * Tạo đơn hàng theo mô hình fire-and-forget:
     * publish event → trả response ngay, không chờ downstream.
     *
     * @param request thông tin đơn hàng từ client
     * @return Mono<OrderResponse> chứa orderId vừa được tạo
     */
    public Mono<OrderResponse> createOrder(CreateOrderRequest request) {
        // Bước 1: tạo orderId dạng ORD-xxxxxxxx (8 hex chars)
        String orderId = "ORD-" + UUID.randomUUID().toString().replace("-", "").substring(0, 8).toUpperCase();
        log.info("Generated orderId={}", orderId);

        // Bước 2: build event
        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .eventType("order.created")
                .orderId(orderId)
                .customerId(request.getCustomerId())
                .productId(request.getProductId())
                .quantity(request.getQuantity())
                .totalAmount(request.getTotalAmount())
                .build();

        // Bước 3: publish event (non-blocking Mono)
        // onErrorResume: nếu Kafka lỗi, không throw về client —
        // order đã được "accepted", lỗi Kafka đã được log ở producer.
        // Tuỳ business requirement có thể đổi thành propagate error.
        return orderEventProducer.publishOrderCreatedEvent(event)
                .doOnSubscribe(s -> log.info("Publishing order.created event | orderId={}", orderId))
                .onErrorResume(ex -> {
                    // Kafka publish thất bại — đã log ở producer.
                    // Vẫn trả 202 vì order đã được accept vào hệ thống.
                    // (Nếu muốn strict, xoá dòng này để propagate 500)
                    log.warn("Kafka publish failed for orderId={}, proceeding with 202", orderId);
                    return Mono.empty();
                })
                // Bước 4: trả response dù publish thành công hay không
                .thenReturn(new OrderResponse(orderId))
                .doOnSuccess(r -> log.info("Order accepted | orderId={}", r.getOrderId()));
    }
}
