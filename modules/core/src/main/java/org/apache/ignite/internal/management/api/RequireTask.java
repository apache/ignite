package org.apache.ignite.internal.management.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;

/**
 * Annotation to specify task classes related to a command.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface RequireTask {
    /** @return Task classes related to a command. */
    public Class<? extends VisorMultiNodeTask<?, ?, ?>>[] value();
}
