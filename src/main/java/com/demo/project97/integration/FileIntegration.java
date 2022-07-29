package com.demo.project97.integration;

import java.io.File;
import java.util.List;

import com.demo.project97.domain.Customer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.file.FileNameGenerator;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.support.FileExistsMode;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class FileIntegration {

    private final DataTransformer dataTransformer;

    /**
     * Detects if new file present in folder, checks every 5 seconds
     * Write the file name to fileChannel1
     */
    @Bean
    public IntegrationFlow readFile(MessageChannel fileChannel1) {
        return IntegrationFlows.from(Files.inboundAdapter(new File("/tmp/src"))
                .autoCreateDirectory(true)
                .preventDuplicates(true)
                .patternFilter("*.txt")
                .get(), poller -> poller.poller(pm -> pm.fixedRate(5000)))
                .transform(dataTransformer, "convertFileToCustomers")
                .handle(message -> {
                    List<Customer> customers = (List<Customer>) message.getPayload();
                    log.info("Customers: {}", customers);
                    for (Customer c: customers) {
                        fileChannel1.send(MessageBuilder.withPayload(c).build());
                    }
                })
                .get();
    }

    @Bean
    public IntegrationFlow readResultChannelWriteToFile() {
        return IntegrationFlows.from("fileChannel2")
                .transform(dataTransformer, "convertDbRecordToString")
                .handle(Files.outboundAdapter(new File("/tmp/des"))
                        .autoCreateDirectory(true)
                        .fileNameGenerator(fileNameGenerator())
                        .fileExistsMode(FileExistsMode.APPEND)
                        .appendNewLine(true)
                        .get())
                .get();
    }

    private FileNameGenerator fileNameGenerator() {
        return new FileNameGenerator() {
            @Override
            public String generateFileName(Message<?> message) {
                return message.getHeaders().get("file-name").toString().concat(".txt");
            }
        };
    }

    @Bean
    public MessageChannel fileChannel1() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel fileChannel2() {
        return new DirectChannel();
    }
}
