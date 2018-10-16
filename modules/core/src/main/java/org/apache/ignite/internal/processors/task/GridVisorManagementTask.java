package org.apache.ignite.internal.processors.task;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that annotated task is a visor task that was invoked by user. They can be handled by event listeners.
 *
 * This annotation intended for internal use only.
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface GridVisorManagementTask {
    // No-op.
}
