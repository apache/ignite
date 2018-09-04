package org.apache.ignite.cache.affinity;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation marker which identifies affinity function that requires previous affinity state
 * in order to calculate new one.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface AffinityDependsOnPreviousState {
}
