package com.lrenyi.spring.nats;

import com.lrenyi.spring.nats.annotations.Subscribe;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.annotation.AnnotationUtils;

@Slf4j
public class NatsBeanPostProcessor implements BeanPostProcessor {
    private final ConnectionHolder connectionHolder;
    @Getter
    private Map<Connection, List<Object>> resubscribes = new HashMap<>();
    
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
                Optional<Connection> connectionOptional = connectionHolder.getValidateConnection();
                if (connectionOptional.isEmpty()) {
                    throw new InvalidParameterException("the connection of nats is null when create dispatcher.");
                }
                Connection connection = connectionOptional.get();
                dispatcherSubscribe(bean, method, sub.value(), connection);
            });
        });
        return bean;
    }
    
    public void dispatcherSubscribe(Object bean, Method method, String sub, Connection connection) {
        Dispatcher dispatcher = connection.createDispatcher(message -> {
            try {
                method.invoke(bean, message);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new InvalidParameterException(String.format("error for method invoke: %s", method.getName()));
            }
        });
        dispatcher.subscribe(sub);
        List<Object> objects = resubscribes.computeIfAbsent(connection, k -> new ArrayList<>());
        objects.add(bean);
        objects.add(method);
        objects.add(sub);
    }
}
