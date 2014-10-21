/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.query;

import org.gridgain.grid.events.*;

/**
 * Cache query type.
 * <p>
 * Used in {@link GridCacheQueryExecutedEvent} and {@link GridCacheQueryReadEvent}
 * to identify the type of query for which an event was fired.
 *
 * @see GridCacheQueryExecutedEvent#queryType()
 * @see GridCacheQueryReadEvent#queryType()
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
