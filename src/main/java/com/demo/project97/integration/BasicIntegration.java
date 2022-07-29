package com.demo.project97.integration;

import java.util.Arrays;
import java.util.List;

import com.demo.project97.domain.Customer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.BridgeFrom;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.store.SimpleMessageStore;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@IntegrationComponentScan
public class BasicIntegration {

    @Bean
    public IntegrationFlow performSplit() {
        return IntegrationFlows.from("inputChannel1")
                .split()
                .transform("Hello "::concat)
                .handle(message -> {
                    log.info("performSplit: {}", message);
                })
                .get();
    }

    @Bean
    public IntegrationFlow performAggregate(SimpleMessageStore messageStore) {
        return IntegrationFlows.from("inputChannel2")
                .split()
                .aggregate(a ->
                        a.correlationStrategy(m -> {
                            Customer customer = (Customer) m.getPayload();
                            return customer.getCity();
                        })
                                .releaseStrategy(g -> g.size() > 2)
                                .messageStore(messageStore))
                .handle(message -> {
                    log.info("performAggregate: {}", message);
                })
                .get();
    }

    @Bean
    public IntegrationFlow performRoute() {
        return IntegrationFlows.from("inputChannel3")
                .split()
                .log()
                .route(Customer.class, m -> m.getCity(), m -> m
                        .channelMapping("New York", "channelA")
                        .channelMapping("Bangalore", "channelB"))
                .get();
    }

    @Bean
    public IntegrationFlow handleNewYork() {
        return IntegrationFlows.from("channelA")
                .handle(message -> {
                    log.info("handleNewYork: {}", message);
                })
                .get();
    }

    @Bean
    public IntegrationFlow handleBangalore() {
        return IntegrationFlows.from("channelB")
                .handle(message -> {
                    log.info("handleBangalore: {}", message);
                })
                .get();
    }

    @Bean
    public IntegrationFlow performSubFlow(IntegrationFlow subFlowNewYork, IntegrationFlow subFlowBangalore) {
        return IntegrationFlows.from("inputChannel4")
                .split()
                .log()
                .route(Customer.class, m -> m.getCity(), m -> m
                        .subFlowMapping("New York", subFlowNewYork)
                        .subFlowMapping("Bangalore", subFlowBangalore))
                .get();
    }

    @Bean
    public IntegrationFlow subFlowNewYork() {
        return f -> f.handle(m -> log.info("subFlowNewYork: {}", m));
    }

    @Bean
    public IntegrationFlow subFlowBangalore() {
        return f -> f.handle(m -> log.info("subFlowBangalore: {}", m));
    }

    @Bean
    public IntegrationFlow performBridge() {
        return IntegrationFlows.from("polledChannel")
                .bridge(e -> e.poller(Pollers.fixedDelay(5000).maxMessagesPerPoll(10)))
                .handle(message -> {
                    log.info("performBridge: {}", message);
                })
                .get();
    }

    @Bean
    public IntegrationFlow readInputChannel5_sub1() {
        return IntegrationFlows.from("inputChannel5_sub1")
                .handle(message -> {
                    log.info("readInputChannel5_sub1: {}", message);
                })
                .get();
    }

    @Bean
    public IntegrationFlow readInputChannel5_sub2() {
        return IntegrationFlows.from("inputChannel5_sub2")
                .handle(message -> {
                    log.info("readInputChannel5_sub2: {}", message);
                })
                .get();
    }

    @Bean
    public IntegrationFlow performDynamicBridge() {
        List<String> cities = Arrays.asList("New York", "Bangalore", "London");
        return IntegrationFlows.from("inputChannel6")
                .split()
                .route(Customer.class, m -> m.getCity(), m -> {
                    cities.forEach(city -> {
                        m.subFlowMapping(city, subFlow -> subFlow.publishSubscribeChannel(c -> {
                            c.ignoreFailures(true);
                            c.subscribe(s -> s.handle(h -> {
                                Customer customer = (Customer) h.getPayload();
                                customer.setName(customer.getName().toUpperCase());
                                log.info("Handle: {}", customer);
                            }));
                        }).bridge());
                    });
                })
                .aggregate()
                .handle(m -> {
                    log.info("performDynamicBridge: {}", m);
                })
                .get();
    }

    @Bean
    public MessageChannel inputChannel1() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel inputChannel2() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel inputChannel3() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel inputChannel4() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel inputChannel5() {
        return new PublishSubscribeChannel();
    }

    @Bean
    @BridgeFrom("inputChannel5")
    public MessageChannel inputChannel5_sub1() {
        return new DirectChannel();
    }

    @Bean
    @BridgeFrom("inputChannel5")
    public MessageChannel inputChannel5_sub2() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel inputChannel6() {
        return new DirectChannel();
    }

    @Bean
    public PollableChannel polledChannel() {
        return new QueueChannel();
    }

    @Bean
    public SimpleMessageStore messageStore() {
        return new SimpleMessageStore();
    }

    @Bean
    public MessageChannel channelA() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel channelB() {
        return new DirectChannel();
    }
}
