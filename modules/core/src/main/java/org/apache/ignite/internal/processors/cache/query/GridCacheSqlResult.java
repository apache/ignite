/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.query;

import java.util.*;

/**
 * TODO replace with {@link org.apache.ignite.cache.query.QueryCursor}
 *
 * SQL Query result.
 */
public interface GridCacheSqlResult extends AutoCloseable, Iterable<List<?>> {
    // No-op.
}
