package com.lrenyi.spring.nats;

import io.nats.client.Connection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionHolder {
    private final AtomicInteger next = new AtomicInteger(0);
    
    public Connection getValidateConnection() {
        List<Connection> allConnection = NatsConfiguration.findAllConnection();
        if (allConnection.isEmpty()) {
            return null;
        }
        int i = next.get();
        Connection nc;
        if (i < allConnection.size()) {
            nc = allConnection.get(i);
        } else {
            next.set(0);
            nc = allConnection.get(0);
        }
        next.incrementAndGet();
        return nc;
    }
}
