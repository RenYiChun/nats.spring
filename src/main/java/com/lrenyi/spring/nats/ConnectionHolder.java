package com.lrenyi.spring.nats;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import com.lrenyi.spring.nats.annotations.Subscribe;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ConnectionHolder implements InitializingBean, BeanPostProcessor,
        ApplicationListener<ApplicationReadyEvent>, DisposableBean {
    public final Lock lock = new ReentrantLock();
    private final AtomicInteger next = new AtomicInteger(0);
    private final List<Connection> allConn = new ArrayList<>();
    private final Set<Connection> ownedConnections = new HashSet<>();
    private final Set<SubscribeInfo> pendingSubscriptions = new HashSet<>();
    private final Map<Connection, Set<SubscribeInfo>> resubscribes = new HashMap<>();
    private final Map<Connection, List<Dispatcher>> dispatchers = new HashMap<>();
    private ScheduledExecutorService scheduler;
    private NatsProperties properties;
    private volatile boolean applicationReady;
    private volatile boolean destroyed;

    @Autowired(required = false)
    public void setConnection(Connection connection) {
        if (connection == null) {
            return;
        }
        lock.lock();
        try {
            allConn.add(connection);
        } finally {
            lock.unlock();
        }
    }

    @Autowired
    public void setProperties(NatsProperties properties) {
        this.properties = properties;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (!NatsConfiguration.hasServer(properties) && findAllConnection().isEmpty()) {
            log.warn("skip nats connection status checker because server url is not configured.");
            return;
        }
        try {
            if (properties.isReconnectWhenClosed()) {
                createConfiguredConnections();
                startStatusCheckerThread();
            }
        } catch (Exception error) {
            cleanupAfterInitializationFailure();
            throw error;
        } catch (Error error) {
            cleanupAfterInitializationFailure();
            throw error;
        }
    }

    private void cleanupAfterInitializationFailure() {
        lock.lock();
        try {
            stopStatusChecker();
            closeDispatchers();
            closeOwnedConnections();
        } finally {
            lock.unlock();
        }
    }

    private void createConfiguredConnections() throws Exception {
        lock.lock();
        try {
            int missing = Math.max(0, properties.getConnectionTotal() - allConn.size());
            for (int i = 0; i < missing; i++) {
                Connection connection = NatsConfiguration.makeConnection(properties);
                if (connection != null) {
                    allConn.add(connection);
                    ownedConnections.add(connection);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private synchronized void startStatusCheckerThread() {
        if (scheduler != null || destroyed) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(ConnectionHolder::newStatusCheckerThread);
        scheduler.scheduleWithFixedDelay(this::replaceClosedConnections, 1L, 8L, TimeUnit.SECONDS);
    }

    static Thread newStatusCheckerThread(Runnable task) {
        Thread thread = new Thread(task, "nats-connection-status-checker");
        thread.setDaemon(true);
        return thread;
    }

    private void replaceClosedConnections() {
        lock.lock();
        try {
            if (destroyed) {
                return;
            }
            Iterator<Connection> iterator = allConn.iterator();
            List<Connection> newConnections = new ArrayList<>();
            while (iterator.hasNext()) {
                Connection connection = iterator.next();
                if (connection == null || connection.getStatus() != Connection.Status.CLOSED) {
                    continue;
                }
                replaceClosedConnection(iterator, connection, newConnections);
            }
            allConn.addAll(newConnections);
        } finally {
            lock.unlock();
        }
    }

    private void replaceClosedConnection(Iterator<Connection> iterator,
                                         Connection closedConnection,
                                         List<Connection> newConnections) {
        Connection replacement = null;
        Set<SubscribeInfo> subscribeInfos = new HashSet<>(
                resubscribes.getOrDefault(closedConnection, Set.of()));
        try {
            replacement = NatsConfiguration.makeConnection(properties);
            if (replacement == null) {
                return;
            }
            for (SubscribeInfo subscribeInfo : subscribeInfos) {
                dispatcherSubscribeLocked(subscribeInfo, replacement);
            }
            closeDispatchers(closedConnection);
            resubscribes.remove(closedConnection);
            iterator.remove();
            ownedConnections.remove(closedConnection);
            ownedConnections.add(replacement);
            newConnections.add(replacement);
        } catch (Throwable error) {
            log.warn("failed to replace closed nats connection.", error);
            if (replacement != null) {
                closeDispatchers(replacement);
                resubscribes.remove(replacement);
                closeConnection(replacement);
            }
        }
    }

    public void dispatcherSubscribe(Object bean, Method method, Subscribe sub, Connection connection) {
        lock.lock();
        try {
            dispatcherSubscribeLocked(new SubscribeInfo(bean, method, sub), connection);
        } finally {
            lock.unlock();
        }
    }

    private void dispatcherSubscribeLocked(SubscribeInfo subscribeInfo, Connection connection) {
        Dispatcher dispatcher = connection.createDispatcher(message -> {
            try {
                subscribeInfo.getMethod().invoke(subscribeInfo.getBean(), message);
            } catch (IllegalAccessException | InvocationTargetException e) {
                log.error("invoke method[{}] error.", subscribeInfo.getMethod().getName(), e);
            }
        });
        try {
            Subscribe subject = subscribeInfo.getSubject();
            if (subject.queue().isEmpty()) {
                dispatcher.subscribe(subject.value());
            } else {
                dispatcher.subscribe(subject.value(), subject.queue());
            }
            resubscribes.computeIfAbsent(connection, key -> new HashSet<>()).add(subscribeInfo);
            dispatchers.computeIfAbsent(connection, key -> new ArrayList<>()).add(dispatcher);
        } catch (RuntimeException error) {
            connection.closeDispatcher(dispatcher);
            throw error;
        }
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, @NonNull String beanName) throws BeansException {
        if (!NatsConfiguration.hasServer(properties) && findAllConnection().isEmpty()) {
            return bean;
        }
        Arrays.stream(bean.getClass().getMethods()).forEach(method -> {
            Optional<Subscribe> subOpt = Optional.ofNullable(AnnotationUtils.findAnnotation(method, Subscribe.class));
            subOpt.ifPresent(sub -> registerSubscription(bean, method, sub));
        });
        return bean;
    }

    private void registerSubscription(Object bean, Method method, Subscribe sub) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length != 1 || !parameterTypes[0].equals(Message.class)) {
            throw new InvalidParameterException("");
        }
        SubscribeInfo subscribeInfo = new SubscribeInfo(bean, method, sub);
        lock.lock();
        try {
            if (properties.isSubscribeAfterApplicationReady() && !applicationReady) {
                pendingSubscriptions.add(subscribeInfo);
                return;
            }
            subscribe(subscribeInfo);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void onApplicationEvent(@NonNull ApplicationReadyEvent event) {
        lock.lock();
        try {
            applicationReady = true;
            for (SubscribeInfo subscribeInfo : pendingSubscriptions) {
                subscribe(subscribeInfo);
            }
            pendingSubscriptions.clear();
        } finally {
            lock.unlock();
        }
    }

    private void subscribe(SubscribeInfo subscribeInfo) {
        Connection connection = getValidateConnectionLocked()
                .orElseThrow(() -> new InvalidParameterException(
                        "the connection of nats is null when create dispatcher."));
        dispatcherSubscribeLocked(subscribeInfo, connection);
    }

    public Optional<Connection> getValidateConnection() {
        lock.lock();
        try {
            return getValidateConnectionLocked();
        } finally {
            lock.unlock();
        }
    }

    private Optional<Connection> getValidateConnectionLocked() {
        if (allConn.isEmpty()) {
            return Optional.empty();
        }
        int start = Math.floorMod(next.getAndIncrement(), allConn.size());
        for (int offset = 0; offset < allConn.size(); offset++) {
            Connection connection = allConn.get((start + offset) % allConn.size());
            if (connection != null && connection.getStatus() != Connection.Status.CLOSED) {
                return Optional.of(connection);
            }
        }
        return Optional.empty();
    }

    public List<Connection> findAllConnection() {
        lock.lock();
        try {
            return new ArrayList<>(allConn);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void destroy() {
        lock.lock();
        try {
            destroyed = true;
            stopStatusChecker();
            closeDispatchers();
            closeOwnedConnections();
            allConn.clear();
            pendingSubscriptions.clear();
            resubscribes.clear();
        } finally {
            lock.unlock();
        }
    }

    private synchronized void stopStatusChecker() {
        if (scheduler == null) {
            return;
        }
        scheduler.shutdownNow();
        scheduler = null;
    }

    private void closeDispatchers() {
        new ArrayList<>(dispatchers.keySet()).forEach(this::closeDispatchers);
    }

    private void closeDispatchers(Connection connection) {
        List<Dispatcher> connectionDispatchers = dispatchers.remove(connection);
        if (connectionDispatchers == null) {
            return;
        }
        for (Dispatcher dispatcher : connectionDispatchers) {
            try {
                connection.closeDispatcher(dispatcher);
            } catch (Throwable error) {
                log.warn("failed to close nats dispatcher.", error);
            }
        }
    }

    private void closeOwnedConnections() {
        for (Connection connection : new HashSet<>(ownedConnections)) {
            closeConnection(connection);
            allConn.remove(connection);
        }
        ownedConnections.clear();
    }

    private void closeConnection(Connection connection) {
        try {
            connection.close();
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            log.warn("interrupted when closing nats connection.", error);
        } catch (Throwable error) {
            log.warn("failed to close nats connection.", error);
        }
    }
}
