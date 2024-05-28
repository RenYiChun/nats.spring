package com.lrenyi.spring.nats;

import com.lrenyi.spring.nats.annotations.Subscribe;
import io.nats.client.Connection;
import io.nats.client.Message;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class NatsConnectionTest {
    
    @Test
    public void testReConnection() throws Exception {
        NatsProperties properties = new NatsProperties();
        properties.setServer("nats://localhost:4222");
        properties.setConnectionTotal(2);
        
        ConnectionHolder holder = new ConnectionHolder();
        holder.setProperties(properties);
        holder.afterPropertiesSet();
        
        holder.postProcessAfterInitialization(new TestBean(), "testBean");
        
        while (true) {
            System.out.println("================================================");
            Optional<Connection> connection1Optional = holder.getValidateConnection();
            if (connection1Optional.isEmpty()) {
                Thread.sleep(4000);
                continue;
            }
            Connection connection = connection1Optional.get();
            Connection.Status status = connection.getStatus();
            if (Connection.Status.CLOSED == status) {
                Thread.sleep(4000);
                continue;
            }
            connection.publish("trace-data", "你好".getBytes(StandardCharsets.UTF_8));
            connection.publish("trace-data1", "你好".getBytes(StandardCharsets.UTF_8));
            System.out.println(connection + "->" + status);
            Thread.sleep(4000);
        }
    }
    
    private static class TestBean {
        @Subscribe("trace-data")
        public void sub(Message msg) {
            System.out.println("####A: " + new String(msg.getData()));
        }
        
        @Subscribe("trace-data1")
        public void sub1(Message msg) {
            System.out.println("####B: " + new String(msg.getData()));
        }
    }
}
