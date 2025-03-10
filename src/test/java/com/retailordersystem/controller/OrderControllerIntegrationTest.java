package com.retailordersystem.controller;



import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import com.retailordersystem.model.Order;
import com.retailordersystem.repository.OrderRepository;

@SpringBootTest
@AutoConfigureMockMvc
@DirtiesContext
public class OrderControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private OrderRepository orderRepository;

    @Test
    public void shouldCreateOrderAndPublishEvent() throws Exception {
        // Create a new order
        Order order = new Order();
        order.setStatus("PENDING");

        // Perform POST request to create the order
        MvcResult mvcResult = mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"status": "PENDING","description": "Order from Integration Test "}
                                """))
                .andExpect(status().isCreated())
                .andExpect(content().string(containsString("PENDING")))
                .andReturn();

        // Extract the created order's ID from the response
        String responseContent = mvcResult.getResponse().getContentAsString();
        Long orderId = Long.parseLong(responseContent.substring(responseContent.indexOf("\"id\":") + 5, responseContent.indexOf(",")));

        // Verify order saved in the database
        Order savedOrder = orderRepository.findById(orderId).orElseThrow();
        assertThat(savedOrder.getStatus()).isEqualTo("PENDING");

        // Wait for the order to be processed by the consumer (adjust timeout as needed)
        await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
            Order updatedOrder = orderRepository.findById(orderId).orElseThrow();
            assertThat(updatedOrder.getStatus()).isEqualTo("PROCESSED"); // Assuming "PROCESSED" is the final status
        });
    }
    // ... add more integration tests for other controller methods
}