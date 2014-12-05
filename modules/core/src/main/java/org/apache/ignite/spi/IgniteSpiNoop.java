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
 * Annotates {@code NO-OP} SPI implementations. {@code NO-OP} implementations are empty implementations
 * and sometimes the system may provide optimizations to remove any overhead that may be involved
 * with SPI invocation.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface IgniteSpiNoop {
    // No-op.
}
