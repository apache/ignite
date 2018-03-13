package org.apache.ignite.springdata.repository.support;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Always false condition.
 * Tells spring context never load bean with such Condition.
 *
 * @author Roman_Meerson
 */
public class ConditionFalse implements Condition {
    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        return false;
    }
}
