package com.lrenyi.spring.nats;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.util.StringUtils;

class NatsServerConfiguredCondition implements Condition {
    
    static final String NATS_SERVER_PROPERTY = "app.template.nats.server";
    
    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        return StringUtils.hasText(context.getEnvironment().getProperty(NATS_SERVER_PROPERTY));
    }
}
