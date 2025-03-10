package com.retailordersystem.kafka;

public record OrderPlacedEvent (Long orderId,String orderStatus) {}