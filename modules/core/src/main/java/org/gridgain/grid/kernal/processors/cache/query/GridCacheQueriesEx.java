/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.query.*;

import java.util.*;

/**
 * Extended queries interface.
 */
public interface GridCacheQueriesEx<K, V> extends GridCacheQueries<K, V> {
    /**
     * Gets SQL metadata.
     *
     * @return SQL metadata.
     * @throws IgniteCheckedException In case of error.
     */
    public Collection<GridCacheSqlMetadata> sqlMetadata() throws IgniteCheckedException;

    /**
     * Creates SQL fields query which will include results metadata if needed.
     *
     * @param qry SQL query.
     * @param incMeta Whether to include results metadata.
     * @return Created query.
     */
    public GridCacheQuery<List<?>> createSqlFieldsQuery(String qry, boolean incMeta);

    /**
     * Creates SPI query.
     *
     * @return Query.
     */
    public <R> GridCacheQuery<R> createSpiQuery();

    /**
     * @param qry Query.
     * @return Future.
     */
    public IgniteFuture<GridCacheSqlResult> execute(GridCacheTwoStepQuery qry);
}
