/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.query;

/**
 * Cache query type.
 * <p>
 * Used in {@link org.apache.ignite.events.GridCacheQueryExecutedEvent} and {@link org.apache.ignite.events.GridCacheQueryReadEvent}
 * to identify the type of query for which an event was fired.
 *
 * @see org.apache.ignite.events.GridCacheQueryExecutedEvent#queryType()
 * @see org.apache.ignite.events.GridCacheQueryReadEvent#queryType()
 */
public enum GridCacheQueryType {
    /** SQL query. */
    SQL,

    /** SQL fields query. */
    SQL_FIELDS,

    /** Full text query. */
    FULL_TEXT,

    /** Scan query. */
    SCAN,

    /** Continuous query. */
    CONTINUOUS
}
