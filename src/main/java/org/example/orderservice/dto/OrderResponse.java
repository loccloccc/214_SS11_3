package org.example.orderservice.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response trả về cho client sau khi đơn hàng được accept.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderResponse {

    private String orderId;
}
