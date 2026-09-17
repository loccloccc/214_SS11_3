package org.example.orderservice.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.orderservice.dto.CreateOrderRequest;
import org.example.orderservice.dto.OrderResponse;
import org.example.orderservice.service.OrderService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * REST Controller cho Order API.
 *
 * Endpoint: POST /api/v1/orders
 *
 * Yêu cầu WebFlux Non-blocking:
 *   - Trả về Mono<OrderResponse> — không bao giờ gọi .block()
 *   - HTTP 202 Accepted ngay lập tức sau khi publish event
 *   - Không chờ downstream processing (trừ kho, thanh toán, …)
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    /**
     * Tạo đơn hàng mới.
     *
     * HTTP 202 Accepted: request đã được nhận và event đã được đẩy vào Kafka.
     * Quá trình xử lý thực tế (trừ kho, trừ tiền) diễn ra bất đồng bộ
     * bởi các downstream services lắng nghe topic storex-order-events.
     *
     * @param request validated request body
     * @return Mono<OrderResponse> với orderId vừa tạo
     */
    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)   // 202
    public Mono<OrderResponse> createOrder(@Valid @RequestBody CreateOrderRequest request) {
        log.info("Received create order request | customerId={} | productId={} | quantity={}",
                request.getCustomerId(), request.getProductId(), request.getQuantity());

        // Delegate hoàn toàn cho service — controller không biết gì về Kafka
        // Không dùng .block() — đây là reactive pipeline thuần tuý
        return orderService.createOrder(request);
    }
}
