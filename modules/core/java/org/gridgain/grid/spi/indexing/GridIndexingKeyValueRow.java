// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing;

import java.util.*;

/**
 * This class represents a single row returned by key-value query. For example, it is returned
 * by query such as {@link GridIndexingSpi#query(String, String, Collection, GridIndexingTypeDescriptor, GridIndexingQueryFilter[])}
 * method. Key-value queries are different from fields query in a way that they
 * return the whole cached value, not its individual fields.
 * See also {@link GridIndexingSpi#query(String, String, Collection, GridIndexingTypeDescriptor, GridIndexingQueryFilter[])}.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridIndexingKeyValueRow<K, V> {
    /**
     * Gets cache key.
     *
     * @return Cache key.
     */
    public GridIndexingEntity<K> key();

    /**
     * Gets cache value.
     *
     * @return Cache value.
     */
    public GridIndexingEntity<V> value();

    /**
     * Gets version of cache value.
     *
     * @return Version of cache value.
     */
    public byte[] version();
}
