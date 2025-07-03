package com.lrenyi.spring.nats;

import com.lrenyi.template.core.util.StringUtils;
import java.util.ServiceLoader;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventInitService {
    public static String EVENT_SOURCE;
    
    public static void init() {
        EVENT_SOURCE = System.getenv("spring.application.name");
        if (!StringUtils.hasLength(EVENT_SOURCE)) {
            EVENT_SOURCE = UUID.randomUUID().toString();
        }
        log.info("event source is: {}", EVENT_SOURCE);
        ServiceLoader<EventProcessor> loader = ServiceLoader.load(EventProcessor.class);
        for (EventProcessor processor : loader) {
            EventProcessor.ALL_EVENT_PROCESSOR.put(processor.getEventType(), processor);
            log.info("discover event processor: {}", processor.getEventType());
        }
    }
}
