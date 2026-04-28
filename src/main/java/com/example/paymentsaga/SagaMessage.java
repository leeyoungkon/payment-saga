package com.example.paymentsaga;


import java.math.BigDecimal;

public class SagaMessage {
    private String sagaId;
    private String eventType;
    private String orderId;
    private String productId;
    private Integer quantity;
    private BigDecimal amount;
    private String reason;

    public SagaMessage() {}

    public SagaMessage(String sagaId, String eventType, String orderId,
                       String productId, Integer quantity,
                       BigDecimal amount, String reason) {
        this.sagaId = sagaId;
        this.eventType = eventType;
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
        this.amount = amount;
        this.reason = reason;
    }

    public String getSagaId() { return sagaId; }
    public void setSagaId(String sagaId) { this.sagaId = sagaId; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getProductId() { return productId; }
    public void setProductId(String productId) { this.productId = productId; }

    public Integer getQuantity() { return quantity; }
    public void setQuantity(Integer quantity) { this.quantity = quantity; }

    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }

    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }

    @Override
    public String toString() {
        return "SagaMessage{" +
                "sagaId='" + sagaId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", orderId='" + orderId + '\'' +
                ", productId='" + productId + '\'' +
                ", quantity=" + quantity +
                ", amount=" + amount +
                ", reason='" + reason + '\'' +
                '}';
    }
}