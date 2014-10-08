/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design.queries.annotations;

import org.gridgain.grid.cache.query.*;

import java.lang.annotation.*;

/**
 * Annotation for fields or getters to be indexed for full text
 * search using {@code H2 TEXT} indexing. For more information
 * refer to {@link GridCacheQuery} documentation.
 * @see GridCacheQuery
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.TYPE})
public @interface GridCacheQueryTextField {
    // No-op.
}
