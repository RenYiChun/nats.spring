package com.lrenyi.spring.nats;

import static org.assertj.core.api.Assertions.assertThat;

import com.lrenyi.spring.SpringNatsConfig;
import com.lrenyi.spring.nats.annotations.Subscribe;
import io.nats.client.Connection;
import io.nats.client.Message;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

class NatsMissingServerTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(SpringNatsConfig.class, SubscribeConfiguration.class);

    @Test
    void startsAndSkipsSubscribeDispatcherWhenServerIsMissing() {
        contextRunner.run(context -> {
            assertThat(context.getStartupFailure()).isNull();
            assertThat(context).hasSingleBean(TestSubscriber.class);
            assertThat(context.containsBean("natsConnection")).isFalse();
            assertThat(context).doesNotHaveBean(Connection.class);
        });
    }

    @Test
    void startsAndSkipsSubscribeDispatcherWhenServerIsBlank() {
        contextRunner
                .withPropertyValues("app.template.nats.server=")
                .run(context -> {
                    assertThat(context.getStartupFailure()).isNull();
                    assertThat(context).hasSingleBean(TestSubscriber.class);
                    assertThat(context.containsBean("natsConnection")).isFalse();
                    assertThat(context).doesNotHaveBean(Connection.class);
                });
    }

    @Configuration(proxyBeanMethods = false)
    static class SubscribeConfiguration {

        @Bean
        TestSubscriber testSubscriber() {
            return new TestSubscriber();
        }
    }

    static class TestSubscriber {

        @Subscribe("test-subject")
        public void onMessage(Message message) {
        }
    }
}
