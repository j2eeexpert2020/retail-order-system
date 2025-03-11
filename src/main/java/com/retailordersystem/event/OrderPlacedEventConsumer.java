package com.retailordersystem.event;

//In KafkaConfig.java or a separate consumer class
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.retailordersystem.kafka.OrderPlacedEvent;
import com.retailordersystem.model.Order;
import com.retailordersystem.repository.OrderRepository;

@Component
public class OrderPlacedEventConsumer {

 private final OrderRepository orderRepository;

 public OrderPlacedEventConsumer(OrderRepository orderRepository) {
     this.orderRepository = orderRepository;
 }

 @KafkaListener(topics = "order_placed_topic")
 public void handleOrderPlacedEvent(OrderPlacedEvent  event) {
     try {
         Long orderId = event.orderId();
         Order order = orderRepository.findById(orderId)
                 .orElseThrow(() -> new RuntimeException("Order not found with ID: " + orderId));

         // --- Perform Order Processing Logic ---
         // This is where you'd simulate inventory checks, etc.
         // For now, let's just update the status to "PROCESSED"
         order.setStatus("PROCESSED"); 
         orderRepository.save(order);

     } catch (NumberFormatException ex) {
        // System.err.println("Invalid order ID received: " + event.orderId());
         // Handle the error appropriately (e.g., log, send to a dead-letter queue)
     } catch (RuntimeException ex) {
         //System.err.println("Error processing order: " + ex.getMessage());
         // Handle the error (e.g., publish OrderFailedEvent)
     }
 }
}