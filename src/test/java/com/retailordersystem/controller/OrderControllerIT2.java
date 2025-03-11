package com.retailordersystem.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Duration;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.retailordersystem.kafka.OrderPlacedEvent;
import com.retailordersystem.model.Order;
import com.retailordersystem.repository.OrderRepository;

@SpringBootTest
@AutoConfigureMockMvc
@ExtendWith(SpringExtension.class)
@Testcontainers
public class OrderControllerIT2 {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private ConsumerFactory<String, OrderPlacedEvent> consumerFactory; // Injected ConsumerFactory

    private static final ObjectMapper objectMapper = new ObjectMapper();

    static PostgreSQLContainer<?> postgreSQLContainer = 
        new PostgreSQLContainer<>(DockerImageName.parse("postgres:15"));

    static KafkaContainer kafkaContainer = 
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.2"));

    @BeforeAll
    static void startContainers() {
        postgreSQLContainer.start();
        kafkaContainer.start();
    }

    @AfterAll
    static void stopContainers() {
        postgreSQLContainer.stop();
        kafkaContainer.stop();
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgreSQLContainer::getJdbcUrl);
        registry.add("spring.datasource.username", postgreSQLContainer::getUsername);
        registry.add("spring.datasource.password", postgreSQLContainer::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);
    }

    @Test
    void testCreateOrder() throws Exception {
    	orderRepository.deleteAll();
        // Given: Order request payload
        Order order = new Order();
        order.setStatus("NEW");

        String orderJson = objectMapper.writeValueAsString(order);

        // When & Then: Call API and verify response
        mockMvc.perform(post("/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .content(orderJson))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("NEW"));

        // Verify Order saved in DB
        List<Order> orders = orderRepository.findAll();
        assertThat(orders).hasSize(1);
        assertThat(orders.get(0).getStatus()).isEqualTo("NEW");

        // ✅ Verify Kafka event using ConsumerFactory
        OrderPlacedEvent receivedEvent = consumeKafkaEvent("order_placed_topic");
        assertThat(receivedEvent).isNotNull();
        assertThat(receivedEvent.orderId()).isEqualTo(orders.get(0).getId());
        assertThat(receivedEvent.orderStatus()).isEqualTo("PROCESSED");
    }

    private OrderPlacedEvent consumeKafkaEvent(String topic) {
        try (KafkaConsumer<String, OrderPlacedEvent> consumer = (KafkaConsumer<String, OrderPlacedEvent>) consumerFactory.createConsumer()) {
            
            // Manually set Kafka bootstrap server from Testcontainers
            consumer.unsubscribe();  // Ensure clean subscription
            consumer.subscribe(List.of(topic));

            OrderPlacedEvent event = null;

            // Poll multiple times to ensure Kafka message retrieval
            for (int i = 0; i < 5; i++) { // Poll up to 5 times
                ConsumerRecords<String, OrderPlacedEvent> records = consumer.poll(Duration.ofSeconds(5));

                for (ConsumerRecord<String, OrderPlacedEvent> record : records) {
                    event = record.value();
                    break; // Stop after first event
                }

                if (event != null) break; // Exit loop if event is received
            }

            return event;
        }
    }

}
