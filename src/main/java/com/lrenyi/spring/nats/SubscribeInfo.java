package com.lrenyi.spring.nats;

import java.lang.reflect.Method;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SubscribeInfo {
    private Object bean;
    private Method method;
    private String subject;
    
    public SubscribeInfo() {
    }
    
    public SubscribeInfo(Object bean, Method method, String subject) {
        this.bean = bean;
        this.method = method;
        this.subject = subject;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(bean, method, subject);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {return true;}
        if (!(o instanceof SubscribeInfo that)) {return false;}
        //@formatter:off
        return Objects.equals(bean, that.bean)
                && Objects.equals(method, that.method)
                && Objects.equals(subject,that.subject);
        //@formatter:on
    }
}
