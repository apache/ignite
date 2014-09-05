/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery;

import java.lang.annotation.*;

/**
 * This annotation is for all implementations of {@link GridDiscoverySpi} that support
 * disconnect and reconnect operations.
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Deprecated
public @interface GridDiscoverySpiReconnectSupport {
    /**
     * Whether or not target SPI supports node startup order.
     */
    @SuppressWarnings({"JavaDoc"})
    public boolean value();
}
