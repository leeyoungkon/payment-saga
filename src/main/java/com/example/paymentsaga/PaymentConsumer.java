package com.example.paymentsaga;

import com.example.paymentsaga.SagaMessage;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

@Component
public class PaymentConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentConsumer.class);

    private final KafkaTemplate<String, SagaMessage> kafkaTemplate;
    private final String paymentEventsTopic;
    private final BigDecimal paymentLimit;

    public PaymentConsumer(
            KafkaTemplate<String, SagaMessage> kafkaTemplate,
            @Value("${app.kafka.topics.payment-events}") String paymentEventsTopic,
            @Value("${app.payment.limit:50000}") BigDecimal paymentLimit
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.paymentEventsTopic = paymentEventsTopic;
        this.paymentLimit = paymentLimit;
    }

    @KafkaListener(topics = "${app.kafka.topics.stock-events}", groupId = "${app.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory")
    public void consume(SagaMessage message, Acknowledgment acknowledgment) {

        if (message == null || message.getEventType() == null || message.getAmount() == null || message.getOrderId() == null) {
            log.warn("Skip invalid message: {}", message);
            acknowledgment.acknowledge();
            return;
        }

        if ("StockReserved".equals(message.getEventType())) {
            boolean success = message.getAmount().compareTo(paymentLimit) <= 0;

            SagaMessage resultMessage = success
                    ? new SagaMessage(message.getSagaId(), "PaymentCompleted",
                    message.getOrderId(), message.getProductId(),
                    message.getQuantity(), message.getAmount(), null)
                    : new SagaMessage(message.getSagaId(), "PaymentFailed",
                    message.getOrderId(), message.getProductId(),
                    message.getQuantity(), message.getAmount(), "결제 한도 초과");

            try {
                SendResult<String, SagaMessage> sendResult = kafkaTemplate
                        .send(paymentEventsTopic, message.getOrderId(), resultMessage)
                        .get(5, TimeUnit.SECONDS);
                RecordMetadata metadata = sendResult.getRecordMetadata();
                log.info("Published payment event type={} topic={} partition={} offset={}",
                        resultMessage.getEventType(), metadata.topic(), metadata.partition(), metadata.offset());
                acknowledgment.acknowledge();
            } catch (Exception ex) {
                log.error("Failed to publish payment event for orderId={}", message.getOrderId(), ex);
                throw new IllegalStateException("Failed to publish payment event", ex);
            }
        } else {
            acknowledgment.acknowledge();
        }
    }
}