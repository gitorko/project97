package com.demo.project97.integration;

import com.demo.project97.domain.Customer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.core.AttributeAccessor;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class RabbitMQIntegration {

    private final ConnectionFactory connectionFactory;
    private final DataTransformer dataTransformer;

    @Bean
    public IntegrationFlow readFromQueue(MessageChannel rabbitmqChannel1) {
        return IntegrationFlows.from(Amqp.inboundAdapter(connectionFactory, "phone-queue")
                .errorChannel("errorChannel")
                .errorMessageStrategy(new RabbitMQIntegration.MyFatalExceptionStrategy()))
                .transform(dataTransformer, "convertQueuePayloadToCustomer")
                .handle(message -> {
                    log.info("readFromQueue: {}", message);
                    rabbitmqChannel1.send(message);
                })
                .get();
    }

    public static class MyFatalExceptionStrategy implements ErrorMessageStrategy {
        @Override
        public ErrorMessage buildErrorMessage(Throwable payload, AttributeAccessor attributes) {
            throw new AmqpRejectAndDontRequeueException("Error In Message!");
        }
    }

    @Bean
    public MessageChannel rabbitmqChannel1() {
        DirectChannel channel = new DirectChannel();
        channel.setDatatypes(Customer.class);
        return channel;
    }

    @Bean
    public Queue inboundQueue() {
        return new Queue("phone-queue", true, false, false);
    }
}
