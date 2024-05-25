package com.lrenyi.spring.nats;

import io.nats.client.Connection;
import java.io.IOException;
import java.security.GeneralSecurityException;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class NatsConnectionTest {
    
    @Test
    public void testReConnection() throws IOException, InterruptedException, GeneralSecurityException {
        NatsConfiguration configuration = new NatsConfiguration();
        NatsProperties properties = new NatsProperties();
        properties.setServer("nats://localhost:4222");
        properties.setConnectionTotal(2);
        
        configuration.natsConnection(properties);
//        ConnectionHolder holder = new ConnectionHolder();
//        while (true) {
//            System.out.println("================================================");
//            Connection connection = holder.getValidateConnection();
//            Connection.Status status = connection.getStatus();
//            System.out.println(connection + "->" + status);
//            Thread.sleep(4000);
//        }
    }
}
