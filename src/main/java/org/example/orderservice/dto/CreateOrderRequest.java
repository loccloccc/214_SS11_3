package org.example.orderservice.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * DTO nhận từ client khi tạo đơn hàng.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CreateOrderRequest {

    @NotBlank(message = "customerId must not be blank")
    private String customerId;

    @NotBlank(message = "productId must not be blank")
    private String productId;

    @NotNull(message = "quantity must not be null")
    @Min(value = 1, message = "quantity must be at least 1")
    private Integer quantity;

    @NotNull(message = "totalAmount must not be null")
    @DecimalMin(value = "0.0", inclusive = false, message = "totalAmount must be positive")
    private BigDecimal totalAmount;
}
