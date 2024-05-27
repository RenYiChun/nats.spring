package com.lrenyi.spring.nats;

import io.nats.client.Connection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionHolder {
    private final AtomicInteger next = new AtomicInteger(0);
    
    public Optional<Connection> getValidateConnection() {
        List<Connection> allConnection = NatsConfiguration.findAllConnection();
        if (allConnection.isEmpty()) {
            return Optional.empty();
        }
        int i = next.get();
        Connection nc;
        if (i < allConnection.size()) {
            nc = allConnection.get(i);
        } else {
            next.set(0);
            nc = allConnection.getFirst();
        }
        next.incrementAndGet();
        return Optional.of(nc);
    }
}
