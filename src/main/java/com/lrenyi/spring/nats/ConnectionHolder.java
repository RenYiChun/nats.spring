package com.lrenyi.spring.nats;

import com.lrenyi.template.core.util.SpringContextUtil;
import io.nats.client.Connection;
import java.lang.reflect.Method;
import java.util.ArrayList;
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
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ConnectionHolder implements InitializingBean {
    public final Lock lock = new ReentrantLock();
    private final AtomicInteger next = new AtomicInteger(0);
    private final List<Connection> allConn = new ArrayList<>();
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
    
    @Override
    public void afterPropertiesSet() throws Exception {
        NatsBeanPostProcessor processor = SpringContextUtil.getBean(NatsBeanPostProcessor.class);
        startStatusChecker(processor);
    }
    
    public void startStatusChecker(NatsBeanPostProcessor processor) throws Exception {
        startStatusCheckerThread(processor);
        int total = properties.getConnectionTotal();
        if (properties.isReconnectWhenClosed() && allConn.size() < total) {
            for (int i = 0; i < total - 1; i++) {
                Connection connection = NatsConfiguration.makeConnection(properties);
                allConn.add(connection);
            }
        }
    }
    
    private synchronized void startStatusCheckerThread(NatsBeanPostProcessor processor) {
        Runnable runnable = () -> {
            lock.lock();
            Iterator<Connection> iterator = allConn.iterator();
            List<Connection> newConn = new ArrayList<>();
            Map<Connection, List<Object>> resubscribes = processor.getResubscribes();
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
                        processor.dispatcherSubscribe(bean, method, String.valueOf(objects.get(2)), connect);
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
}
