package com.example.paymentsaga;

import com.example.paymentsaga.SagaMessage;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
public class PaymentCommandConsumer {

    private final KafkaTemplate<String, SagaMessage> kafkaTemplate;

    public PaymentCommandConsumer(KafkaTemplate<String, SagaMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "payment-commands", groupId = "payment-service-group",
            containerFactory = "kafkaListenerContainerFactory")
    public void consume(SagaMessage message) {

        if ("ProcessPaymentCommand".equals(message.getEventType())) {
            boolean success = message.getAmount().compareTo(new BigDecimal("50000")) <= 0;

            if (success) {
                kafkaTemplate.send("payment-events", message.getOrderId(),
                        new SagaMessage(message.getSagaId(), "PaymentCompleted",
                                message.getOrderId(), message.getProductId(),
                                message.getQuantity(), message.getAmount(), null));
            } else {
                kafkaTemplate.send("payment-events", message.getOrderId(),
                        new SagaMessage(message.getSagaId(), "PaymentFailed",
                                message.getOrderId(), message.getProductId(),
                                message.getQuantity(), message.getAmount(), "결제 한도 초과"));
            }
        }
    }
}