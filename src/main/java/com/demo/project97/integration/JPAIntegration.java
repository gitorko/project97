package com.demo.project97.integration;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import javax.persistence.EntityManagerFactory;

import com.demo.project97.domain.Customer;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.jpa.dsl.Jpa;
import org.springframework.integration.jpa.support.PersistMode;
import org.springframework.messaging.MessageChannel;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class JPAIntegration {

    private final EntityManagerFactory entityManager;

    /**
     * Continuously reads the customer table every 10 seconds
     */
    @Bean
    public IntegrationFlow readFromDbAdapter() {
        return IntegrationFlows.from(Jpa.inboundAdapter(this.entityManager)
                .jpaQuery("from Customer where phone is not null")
                .maxResults(2)
                .expectSingleResult(false)
                .entityClass(Customer.class), e -> e.poller(Pollers.fixedDelay(Duration.ofSeconds(10))))
                .handle(message -> {
                    log.info("readFromDbAdapter: {}", message);
                })
                .get();
    }

    /**
     * Starts the flow when the id for customer is pushed to dbChannel1
     */
    @Bean
    public IntegrationFlow readFromDbGateway(MessageChannel dbChannel2, MessageChannel dbChannel3) {
        return IntegrationFlows.from("dbChannel1")
                .handle(Jpa.retrievingGateway(this.entityManager)
                        .jpaQuery("from Customer c where c.id = :id")
                        .expectSingleResult(true)
                        .parameterExpression("id", "payload[id]"))
                .handle(message -> {
                    log.info("readFromDbGateway: {}", message);
                    Customer payload = (Customer) message.getPayload();
                    log.info("readFromDbGateway Customer: {}", payload);
                    dbChannel2.send(message);
                    dbChannel3.send(message);
                })
                .get();
    }

    /**
     * Reads dbChannel2 and updates phone number
     * Doesnt return anything
     */
    @Bean
    public IntegrationFlow updateDbAdapter() {
        return IntegrationFlows.from("dbChannel2")
                .handle(Jpa.outboundAdapter(this.entityManager)
                        .jpaQuery("update Customer c set c.phone = '88888' where c.id =:id")
                        .parameterExpression("id", "payload.id"), e -> e.transactional())
                .get();
    }

    /**
     * Reads dbChannel2 and updates phone number
     * Doesnt return anything
     */
    @Bean
    public IntegrationFlow updateDbGateway() {
        return IntegrationFlows.from("dbChannel3")
                .handle(Jpa.updatingGateway(this.entityManager)
                        .jpaQuery("update Customer c set c.name = CONCAT('Mr. ',c.name) where c.id =:id")
                        .parameterExpression("id", "payload.id"), e -> e.transactional())
                .handle(message -> {
                    log.info("updateDbGateway: {}", message);
                })
                .get();
    }

    /**
     * Reads dbChannel3 and deletes the customer
     */
    @Bean
    public IntegrationFlow deleteRecord() {
        return IntegrationFlows.from(Jpa.inboundAdapter(this.entityManager)
                .jpaQuery("from Customer where name like 'Mr.%'")
                .maxResults(2)
                .expectSingleResult(false)
                .entityClass(Customer.class), e -> e.poller(Pollers.fixedDelay(Duration.ofSeconds(10))))
                .handle(Jpa.outboundAdapter(this.entityManager)
                        .persistMode(PersistMode.DELETE)
                        .parameterExpression("id", "payload.id")
                        .entityClass(Customer.class), e -> e.transactional())
                .get();
    }

    /**
     * Reads the fileChannel1 and persists all customers
     */
    @Bean
    public IntegrationFlow readFileChannelWriteToDb() {
        return IntegrationFlows.from("fileChannel1")
                .handle(Jpa.outboundAdapter(this.entityManager)
                        .entityClass(Customer.class)
                        .persistMode(PersistMode.PERSIST)
                        .get(), e -> e.transactional())
                .get();
    }

    /**
     * Reads the fileChannel1 and persists all customers
     */
    @Bean
    public IntegrationFlow readRabbitmqChannelUpdateDb() {
        return IntegrationFlows.from("rabbitmqChannel1")
                .handle(Jpa.outboundAdapter(this.entityManager)
                                .jpaQuery("update Customer c set c.phone = :phone where c.name =:name")
                                .parameterExpression("phone", "payload.phone")
                                .parameterExpression("name", "payload.name")
                        , e -> e.transactional())
                .get();
    }

    @SneakyThrows
    public void sleep(int seconds) {
        TimeUnit.SECONDS.sleep(seconds);
    }

    @Bean
    public MessageChannel dbChannel1() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel dbChannel2() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel dbChannel3() {
        return new DirectChannel();
    }


}
