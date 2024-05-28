package com.lrenyi.spring.nats;

import io.nats.client.Connection;
import io.nats.client.Consumer;
import io.nats.client.ErrorListener;
import io.nats.client.Nats;
import io.nats.client.Options;
import java.io.IOException;
import java.security.GeneralSecurityException;
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
        return makeConnection(properties);
    }
    
    public static Connection makeConnection(NatsProperties properties) throws IOException, GeneralSecurityException,
            InterruptedException {
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
        } catch (Exception e) {
            log.info("error connecting to nats", e);
            throw e;
        }
        return nc;
    }
    
    @Bean
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    public NatsBeanPostProcessor configNatsBeanPostProcessor(ConnectionHolder connectionHolder) {
        return new NatsBeanPostProcessor(connectionHolder);
    }
}