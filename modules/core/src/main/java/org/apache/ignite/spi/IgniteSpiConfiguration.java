/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi;

import java.lang.annotation.*;

/**
 * Annotates SPI configuration setters on whether or not it is optional.
 * <p>
 * Note that this annotation is used only for documentation purposes now and is not checked by
 * GridGain implementation.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface IgniteSpiConfiguration {
    /**
     * Whether this configuration setting is optional or not.
     */
    @SuppressWarnings("JavaDoc")
    public boolean optional();
}
