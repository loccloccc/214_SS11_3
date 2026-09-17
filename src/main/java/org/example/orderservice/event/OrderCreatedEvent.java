package org.example.orderservice.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * Kafka event được publish khi một đơn hàng được tạo.
 *
 * Thiết kế event rõ ràng với eventType = "order.created"
 * để consumer phía sau có thể route đúng handler.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCreatedEvent {

    /**
     * Loại event. Luôn = "order.created" trong class này.
     * Các event khác (order.paid, order.shipped, …) sẽ có class riêng
     * nhưng cùng schema eventType để consumer dễ dispatch.
     */
    private String eventType;

    /**
     * ID đơn hàng — cũng là Kafka Key (xem BUG-03).
     * Đảm bảo mọi event của cùng một đơn hàng đi vào cùng partition.
     */
    private String orderId;

    private String customerId;
    private String productId;
    private Integer quantity;
    private BigDecimal totalAmount;
}
