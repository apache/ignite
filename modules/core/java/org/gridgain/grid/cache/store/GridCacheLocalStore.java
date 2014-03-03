/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.store;

import java.lang.annotation.*;

/**
 * Annotation for local {@link GridCacheStore} implementation. "Local" here means that there is no global
 * database behind the grid but each node has an independent one.
 *
 * @author @java.author
 * @version @java.version
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface GridCacheLocalStore {
    // No-op.
}
