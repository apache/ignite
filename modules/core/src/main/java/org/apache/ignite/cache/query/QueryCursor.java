// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.cache.query;

import java.util.*;

/**
 * TODO: Add interface description.
 *
 * @author @java.author
 * @version @java.version
 */
public interface QueryCursor<T> extends Iterable<T> {
    /**
     * Gets all query results and stores them in the collection.
     * Use this method when you know in advance that query result is
     * relatively small and will not cause memory utilization issues.
     *
     * @return Collection containing full query result.
     */
    public Collection<T> getAll();
}
