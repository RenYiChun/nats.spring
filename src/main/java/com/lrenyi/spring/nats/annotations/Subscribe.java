package com.lrenyi.spring.nats.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.core.annotation.AliasFor;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Subscribe {
    
    @AliasFor("subscribe") String value() default "";
    
    @AliasFor("value") String subscribe() default "";
    
    String queue() default "";
}
