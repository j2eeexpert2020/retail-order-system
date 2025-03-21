package com.retailordersystem.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.retailordersystem.kafka.OrderPlacedEvent;
import com.retailordersystem.model.Order;
import com.retailordersystem.repository.OrderRepository;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@Testcontainers
@TestMethodOrder(MethodOrderer.MethodName.class)
public class OrderControllerIntegrationTestV2 {

    @LocalServerPort
    private Integer port;

    private static final Integer TIMEOUT = 120;
    private static final Logger logger = LoggerFactory.getLogger(OrderControllerIntegrationTestV2.class);

    @Container
    private static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    private static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:latest"));

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private ObjectMapper objectMapper;

    private final BlockingQueue<OrderPlacedEvent> receivedEvents = new LinkedBlockingQueue<>();
    
    private static String kafkaBootstrap;

    @BeforeAll
    static void startContainers() {
        final String CYAN = "\u001B[36m"; // Cyan text

        logger.info(CYAN + "🚀 Starting PostgreSQL and Kafka containers...");
        postgres.start();
        kafka.start();

        logger.info(CYAN + "✅ PostgreSQL and Kafka containers started. Waiting for full initialization...");

        // Ensure PostgreSQL is running
        logger.info(CYAN + "🔄 Checking if PostgreSQL is running...");
        Awaitility.await().atMost(Duration.ofSeconds(TIMEOUT)).until(postgres::isRunning);
        logger.info(CYAN + "✅ PostgreSQL is up and running!");

        // Ensure Kafka is running
        logger.info(CYAN + "🔄 Checking if Kafka is running...");
        Awaitility.await().atMost(Duration.ofSeconds(TIMEOUT)).until(kafka::isRunning);
        logger.info(CYAN + "✅ Kafka is up and running!");

        // Ensure Kafka bootstrap server is available
        logger.info(CYAN + "🔄 Verifying Kafka bootstrap servers...");
        Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> !kafka.getBootstrapServers().isEmpty());

        // Fetch Kafka bootstrap servers dynamically from Testcontainers
        kafkaBootstrap = kafka.getBootstrapServers();
        
        // Set it as a system property BEFORE Spring starts
        System.setProperty("spring.kafka.bootstrap-servers", kafkaBootstrap);
        
        logger.info(CYAN + "✅ Dynamically setting Kafka Bootstrap Server: {}", kafkaBootstrap);

        logger.info("🚀 Test environment setup complete! Ready to execute tests.");
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);

        // Using the dynamically assigned Kafka Bootstrap Server
        registry.add("spring.kafka.bootstrap-servers", () -> kafkaBootstrap);
        
        logger.info("✅ Injected Kafka bootstrap server dynamically: {}", kafkaBootstrap);
    }

    /*
    @KafkaListener(topics = "order_placed_topic", groupId = "test-group")
    public void listen(OrderPlacedEvent event) {
        receivedEvents.add(event);
    }
    */

    @AfterAll
    static void stopContainers() {
        postgres.stop();
        kafka.stop();
    }

    @Test
    public void shouldCreateOrderAndVerifyDatabase() throws Exception {
        orderRepository.deleteAll(); // Clean the Order table
        Order order = new Order("PENDING", "Dummy Description");

        mockMvc.perform(
                post("/orders").contentType(MediaType.APPLICATION_JSON).content(objectMapper.writeValueAsString(order)))
                .andExpect(status().isCreated()).andExpect(jsonPath("$.status").value("PENDING"));

        List<Order> orders = orderRepository.findAll();
        assertThat(orders).hasSize(1);
        assertThat(orders.get(0).getStatus()).isEqualTo("PENDING");
    }

    @Test
    public void shouldSendKafkaEventAndVerify() throws Exception {
        Awaitility.await().atMost(Duration.ofSeconds(TIMEOUT)).untilAsserted(() -> {
            OrderPlacedEvent receivedEvent = receivedEvents.poll();
            assertThat(receivedEvent).isNotNull();
            assertThat(receivedEvent.orderStatus()).isEqualTo("PROCESSED");
            assertThat(receivedEvent.description()).isEqualTo("Dummy Description");
        });
    }
}
