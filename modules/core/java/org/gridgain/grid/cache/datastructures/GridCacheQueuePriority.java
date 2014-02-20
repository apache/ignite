// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.datastructures;

import java.lang.annotation.*;

/**
 * Annotates fields or methods allowing to specify priority for queue items.
 * Field or method must return {@code int}. Class can't contain more than one
 * {@code GridCacheQueuePriority} annotation. For more information about distributed
 * cache queues see {@link GridCacheQueue} documentation.
 *
 * @author @java.author
 * @version @java.version
 * @see GridCacheQueue
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
public @interface GridCacheQueuePriority {
    // No-op.
}
