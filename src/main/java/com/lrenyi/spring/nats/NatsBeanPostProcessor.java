package com.lrenyi.spring.nats;

import com.lrenyi.spring.nats.annotations.Subscribe;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import java.lang.reflect.InvocationTargetException;
import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.annotation.AnnotationUtils;

@Slf4j
public class NatsBeanPostProcessor implements BeanPostProcessor {
    private final ConnectionHolder connectionHolder;
    
    public NatsBeanPostProcessor(ConnectionHolder connectionHolder) {
        this.connectionHolder = connectionHolder;
    }
    
    @Override
    public Object postProcessAfterInitialization(Object bean, @NonNull String beanName) throws BeansException {
        final Class<?> clazz = bean.getClass();
        Arrays.stream(clazz.getMethods()).forEach(method -> {
            Optional<Subscribe> subOpt = Optional.ofNullable(AnnotationUtils.findAnnotation(method, Subscribe.class));
            subOpt.ifPresent(sub -> {
                final Class<?>[] parameterTypes = method.getParameterTypes();
                if (parameterTypes.length != 1 || !parameterTypes[0].equals(Message.class)) {
                    throw new InvalidParameterException("");
                }
                Optional<Connection> connection = connectionHolder.getValidateConnection();
                if (connection.isEmpty()) {
                    throw new InvalidParameterException("the connection of nats is null when create dispatcher.");
                }
                Dispatcher dispatcher = connection.get().createDispatcher(message -> {
                    try {
                        method.invoke(bean, message);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        log.error(String.format("error for method invoke: %s", method.getName()), e);
                    }
                });
                dispatcher.subscribe(sub.value());
            });
        });
        return bean;
    }
}
