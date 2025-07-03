package com.lrenyi.spring.nats;

import io.nats.client.Connection;
import java.util.HashMap;
import java.util.Map;

public interface EventProcessor {
    
    Map<String, EventProcessor> ALL_EVENT_PROCESSOR = new HashMap<>();
    
    String getEventType();
    
    String handler(String jsonData, Connection connection) throws Throwable;
}
