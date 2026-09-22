package com.lrenyi.spring.nats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.lrenyi.spring.nats.annotations.Subscribe;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.test.util.ReflectionTestUtils;

class ConnectionHolderLifecycleTest {

    @Test
    void delaysSubscriptionsUntilApplicationReadyWhenConfigured() throws Exception {
        Connection connection = mock(Connection.class);
        Dispatcher dispatcher = mock(Dispatcher.class);
        when(connection.getStatus()).thenReturn(Connection.Status.CONNECTED);
        when(connection.createDispatcher(any())).thenReturn(dispatcher);

        ConnectionHolder holder = newHolder(connection, true);
        holder.postProcessAfterInitialization(new TestSubscriber(), "testSubscriber");

        verify(connection, never()).createDispatcher(any());

        holder.onApplicationEvent(mock(ApplicationReadyEvent.class));

        verify(connection).createDispatcher(any());
        verify(dispatcher).subscribe("test-subject");
        holder.destroy();
        verify(connection).closeDispatcher(dispatcher);
    }

    @Test
    void subscribesImmediatelyByDefaultForBackwardCompatibility() throws Exception {
        Connection connection = mock(Connection.class);
        Dispatcher dispatcher = mock(Dispatcher.class);
        when(connection.getStatus()).thenReturn(Connection.Status.CONNECTED);
        when(connection.createDispatcher(any())).thenReturn(dispatcher);

        ConnectionHolder holder = newHolder(connection, false);
        holder.postProcessAfterInitialization(new TestSubscriber(), "testSubscriber");

        verify(connection).createDispatcher(any());
        verify(dispatcher).subscribe("test-subject");
        holder.destroy();
        verify(connection).closeDispatcher(dispatcher);
    }

    @Test
    void statusCheckerIsDaemonAndStopsWithContext() throws Exception {
        Connection connection = mock(Connection.class);
        when(connection.getStatus()).thenReturn(Connection.Status.CONNECTED);
        ConnectionHolder holder = newHolder(connection, false);

        Thread thread = ConnectionHolder.newStatusCheckerThread(() -> { });
        assertThat(thread.isDaemon()).isTrue();
        assertThat(thread.getName()).isEqualTo("nats-connection-status-checker");

        ScheduledExecutorService scheduler = (ScheduledExecutorService) ReflectionTestUtils.getField(
                holder, "scheduler");
        assertThat(scheduler).isNotNull();

        holder.destroy();

        assertThat(scheduler.isShutdown()).isTrue();
        assertThat(ReflectionTestUtils.getField(holder, "scheduler")).isNull();
        verify(connection, never()).close();
    }

    private ConnectionHolder newHolder(Connection connection,
                                       boolean subscribeAfterApplicationReady) throws Exception {
        NatsProperties properties = new NatsProperties();
        properties.setServer("nats://localhost:4222");
        properties.setConnectionTotal(1);
        properties.setSubscribeAfterApplicationReady(subscribeAfterApplicationReady);

        ConnectionHolder holder = new ConnectionHolder();
        holder.setProperties(properties);
        holder.setConnection(connection);
        holder.afterPropertiesSet();
        return holder;
    }

    static class TestSubscriber {

        @Subscribe("test-subject")
        public void onMessage(Message message) {
        }
    }
}
