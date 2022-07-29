package com.demo.project97.integration;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.demo.project97.domain.Customer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DataTransformer {

    @SneakyThrows
    public Message<List<Customer>> convertFileToCustomers(String filePath) {
        log.info("Reading file: {}", filePath);
        List<String> fileLines = java.nio.file.Files.readAllLines(Paths.get((filePath)));
        List<Customer> customers = new ArrayList<>();
        for (String line : fileLines) {
            String[] split = line.split(",");
            customers.add(Customer.builder()
                    .name(split[1])
                    .city(split[2])
                    .build());
        }
        log.info("Deleting file : {}", filePath);
        java.nio.file.Files.delete(Paths.get((filePath)));
        return MessageBuilder.withPayload(customers).build();
    }

    @SneakyThrows
    public Message<String> convertDbRecordToString(Customer customer) {
        log.info("Customer file data: {}", customer.toString());
        return MessageBuilder.withPayload(customer.toString())
                .setHeader("file-name", customer.getName())
                .build();
    }

    @SneakyThrows
    public Message<Customer> convertQueuePayloadToCustomer(Message<?> message) {
        Customer customer = (Customer) message.getPayload();
        return MessageBuilder.withPayload(customer).build();
    }
}
