package com.demo.project97.controller;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.demo.project97.domain.Customer;
import com.demo.project97.repo.CustomerRepository;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.ResourceUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@EnableIntegration
@IntegrationComponentScan
public class HomeController {

    private final MessageChannel inputChannel1;
    private final MessageChannel inputChannel2;
    private final MessageChannel inputChannel3;
    private final MessageChannel inputChannel4;
    private final MessageChannel inputChannel5;
    private final MessageChannel inputChannel6;
    private final MessageChannel polledChannel;
    private final MessageChannel dbChannel1;
    private final CustomerRepository customerRepo;
    private final RabbitTemplate rabbitTemplate;

    @GetMapping("/send-channel1")
    public boolean sendToChannel1() {
        return inputChannel1.send(MessageBuilder.withPayload(Arrays.asList("Jack", "Adam", "Roger")).build());
    }

    @GetMapping("/send-channel2")
    public boolean sendToChannel2() {
        List<Customer> customers = getCustomers();
        return inputChannel2.send(MessageBuilder.withPayload(customers).build());
    }

    @GetMapping("/send-channel3")
    public boolean sendToChannel3() {
        List<Customer> customers = getCustomers();
        return inputChannel3.send(MessageBuilder.withPayload(customers).build());
    }

    @GetMapping("/send-channel4")
    public boolean sendToChannel4() {
        List<Customer> customers = getCustomers();
        return inputChannel4.send(MessageBuilder.withPayload(customers).build());
    }

    @GetMapping("/send-channel5")
    public boolean sendToChannel5() {
        return inputChannel5.send(MessageBuilder.withPayload("Jack").build());
    }

    @GetMapping("/send-channel6")
    public boolean sendToChannel6() {
        List<Customer> customers = getCustomers();
        return inputChannel6.send(MessageBuilder.withPayload(customers).build());
    }

    @GetMapping("/send-polledchannel1")
    public void sendToPolledChannel() {
        List<String> names = Arrays.asList("Jack", "Adam");
        polledChannel.send(MessageBuilder.withPayload(names).build());
    }

    @GetMapping("/send-dbchannel1")
    public void sendToDbChannel1() {
        Map<String, Long> inputMap = Map.of("id", 1l);
        dbChannel1.send(MessageBuilder.withPayload(inputMap).build());
    }

    @GetMapping("/seed-data")
    public void seedData() {
        customerRepo.save(Customer.builder()
                .city("Bangalore")
                .name("Raj")
                .phone("9999")
                .build());
    }

    @SneakyThrows
    @GetMapping("/file-copy")
    public void copyFile() {
        if (!Files.exists(Path.of("/tmp/src"))) {
            Files.createDirectory(Path.of("/tmp/src"));
        }
        Files.copy(Path.of(ResourceUtils.getFile("classpath:customer-city.txt").getPath()), Path.of("/tmp/src/customer-city.txt"), REPLACE_EXISTING);
    }

    @SneakyThrows
    @GetMapping("/send-queue")
    public void sendToRabbitMq() {
        Customer customer = Customer.builder().name("Jack").phone("444-999-5555").build();
        rabbitTemplate.convertAndSend("phone-queue", customer);
    }

    private List<Customer> getCustomers() {
        List<Customer> customers = new ArrayList<>();
        customers.add(Customer.builder().name("Jack").city("New York").build());
        customers.add(Customer.builder().name("Adam").city("New York").build());
        customers.add(Customer.builder().name("Sally").city("New York").build());

        customers.add(Customer.builder().name("Raj").city("Bangalore").build());
        customers.add(Customer.builder().name("Suresh").city("Bangalore").build());
        customers.add(Customer.builder().name("Ajay").city("Bangalore").build());
        return customers;
    }
}
