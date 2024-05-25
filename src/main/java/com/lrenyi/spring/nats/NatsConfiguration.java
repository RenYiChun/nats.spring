package com.lrenyi.spring.nats;

import io.nats.client.Connection;
import io.nats.client.Consumer;
import io.nats.client.ErrorListener;
import io.nats.client.Nats;
import io.nats.client.Options;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;

/**
 * NatsConfiguration will create a NATS connection from an instance of NatsProperties.
 * A default connection and error handler is provided with basic logging.
 * <p>
 * {@link org.springframework.boot.autoconfigure.EnableAutoConfiguration Auto-configuration} for NATS.
 */
@Slf4j
@Configuration
@ConditionalOnClass({Connection.class})
@EnableConfigurationProperties(NatsProperties.class)
public class NatsConfiguration {
    private static final List<Connection> ALL_CONNECTIONS = new ArrayList<>();
    private static final Lock lock = new ReentrantLock();
    private static boolean started = false;
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    
    public static List<Connection> findAllConnection() {
        lock.lock();
        try {
            return ALL_CONNECTIONS;
        } finally {
            lock.unlock();
        }
    }
    
    @Bean
    @ConditionalOnMissingBean
    public ConnectionHolder connectionHolder() {
        return new ConnectionHolder();
    }
    
    /**
     * @return NATS connection created with the provided properties. If no server URL is set the method will return
     * null.
     *
     * @throws IOException          when a connection error occurs
     * @throws InterruptedException in the unusual case of a thread interruption during connect
     */
    @Bean
    @ConditionalOnMissingBean
    public Connection natsConnection(NatsProperties properties) throws IOException, InterruptedException,
            GeneralSecurityException {
        Connection nc;
        String serverProp = (properties != null) ? properties.getServer() : null;
        if (serverProp == null || serverProp.isEmpty()) {
            log.error("the server url of nats is null.....");
            return null;
        }
        Options.Builder builder = properties.toOptionsBuilder();
        int total = properties.getConnectionTotal();
        try {
            log.info("auto connecting to NATS with properties - " + properties);
            builder = builder.connectionListener((conn, type) -> log.info("nats connection status changed " + type));
            builder = builder.errorListener(new ErrorListener() {
                @Override
                public void errorOccurred(Connection conn, String error) {
                    log.info("nats connection error occurred " + error);
                }
                
                @Override
                public void exceptionOccurred(Connection conn, Exception exp) {
                    log.info("nats connection exception occurred", exp);
                }
                
                @Override
                public void slowConsumerDetected(Connection conn, Consumer consumer) {
                    log.info("nats connection slow consumer detected");
                }
            });
            nc = Nats.connect(builder.build());
            if (properties.isReconnectWhenClosed() && ALL_CONNECTIONS.size() < total) {
                ALL_CONNECTIONS.add(nc);
                for (int i = 0; i < total - 1; i++) {
                    Connection connect = Nats.connect(builder.build());
                    ALL_CONNECTIONS.add(connect);
                }
                startStatusCheckerThread(builder);
            }
        } catch (Exception e) {
            log.info("error connecting to nats", e);
            throw e;
        }
        return nc;
    }
    
    private synchronized void startStatusCheckerThread(Options.Builder builder) {
        if (started) {
            return;
        }
        Runnable runnable = () -> {
            lock.lock();
            Iterator<Connection> iterator = ALL_CONNECTIONS.iterator();
            List<Connection> newConn = new ArrayList<>();
            while (iterator.hasNext()) {
                Connection connection = iterator.next();
                if (connection == null || connection.getStatus() != Connection.Status.CLOSED) {
                    continue;
                }
                try {
                    Connection connect = Nats.connect(builder.build());
                    newConn.add(connect);
                    iterator.remove();
                } catch (Throwable ignore) {}
            }
            ALL_CONNECTIONS.addAll(newConn);
            lock.unlock();
        };
        scheduler.scheduleAtFixedRate(runnable, 1, 8, TimeUnit.SECONDS);
        started = true;
    }
    
    @Bean
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    public NatsBeanPostProcessor configNatsBeanPostProcessor(Connection connection) {
        return new NatsBeanPostProcessor(connection);
    }
}