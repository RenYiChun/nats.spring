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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.NonNull;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.stereotype.Component;

@Component
public class ConnectionHolder implements InitializingBean, BeanPostProcessor {
    public final Lock lock = new ReentrantLock();
    private final AtomicInteger next = new AtomicInteger(0);
    private final List<Connection> allConn = new ArrayList<>();
    private final Map<Connection, List<Object>> resubscribes = new HashMap<>();
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private NatsProperties properties;
    
    @Autowired
    public void setConnection(Connection connection) {
        allConn.add(connection);
    }
    
    @Autowired
    public void setProperties(NatsProperties properties) {
        this.properties = properties;
    }
    
    @Override
    public void afterPropertiesSet() throws Exception {
        startStatusCheckerThread();
        int total = properties.getConnectionTotal();
        if (properties.isReconnectWhenClosed() && allConn.size() < total) {
            for (int i = 0; i < total - 1; i++) {
                Connection connection = NatsConfiguration.makeConnection(properties);
                allConn.add(connection);
            }
        }
    }
    
    private synchronized void startStatusCheckerThread() {
        Runnable runnable = () -> {
            lock.lock();
            Iterator<Connection> iterator = allConn.iterator();
            List<Connection> newConn = new ArrayList<>();
            while (iterator.hasNext()) {
                Connection connection = iterator.next();
                if (connection == null || connection.getStatus() != Connection.Status.CLOSED) {
                    continue;
                }
                List<Object> objects = resubscribes.get(connection);
                try {
                    Connection connect = NatsConfiguration.makeConnection(properties);
                    if (connect == null) {
                        continue;
                    }
                    newConn.add(connect);
                    if (objects != null) {
                        Object bean = objects.get(0);
                        Method method = (Method) objects.get(1);
                        dispatcherSubscribe(bean, method, String.valueOf(objects.get(2)), connect);
                        resubscribes.remove(connection);
                    }
                    iterator.remove();
                } catch (Throwable ignore) {}
            }
            allConn.addAll(newConn);
            lock.unlock();
        };
        scheduler.scheduleAtFixedRate(runnable, 1, 8, TimeUnit.SECONDS);
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
                Optional<Connection> connectionOptional = getValidateConnection();
                if (connectionOptional.isEmpty()) {
                    throw new InvalidParameterException("the connection of nats is null when create dispatcher.");
                }
                Connection connection = connectionOptional.get();
                dispatcherSubscribe(bean, method, sub.value(), connection);
            });
        });
        return bean;
    }
    
    public Optional<Connection> getValidateConnection() {
        List<Connection> allConnection = findAllConnection();
        if (allConnection.isEmpty()) {
            return Optional.empty();
        }
        int i = next.get();
        Connection nc;
        if (i < allConn.size()) {
            nc = allConn.get(i);
        } else {
            next.set(0);
            nc = allConn.getFirst();
        }
        next.incrementAndGet();
        return Optional.of(nc);
    }
    
    public List<Connection> findAllConnection() {
        lock.lock();
        try {
            return allConn;
        } finally {
            lock.unlock();
        }
    }
}
