/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.query;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheMode.LOCAL;

/**
 * Local query manager (for cache in LOCAL cache mode).
 */
public class GridCacheLocalQueryManager<K, V> extends GridCacheQueryManager<K, V> {
    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected boolean onPageReady(
        boolean loc,
        GridCacheQueryInfo qryInfo,
        Collection<?> data,
        boolean finished, Throwable e) {
        GridCacheQueryFutureAdapter fut = qryInfo.localQueryFuture();

        assert fut != null;

        if (e != null)
            fut.onPage(null, null, e, true);
        else
            fut.onPage(null, data, null, finished);

        return true;
    }

    /** {@inheritDoc} */
    @Override protected boolean onFieldsPageReady(boolean loc,
        GridCacheQueryInfo qryInfo,
        @Nullable List<GridQueryFieldMetadata> metaData,
        @Nullable Collection<?> entities,
        @Nullable Collection<?> data,
        boolean finished,
        @Nullable Throwable e) {
        assert qryInfo != null;

        GridCacheLocalFieldsQueryFuture fut = (GridCacheLocalFieldsQueryFuture)qryInfo.localQueryFuture();

        assert fut != null;

        if (e != null)
            fut.onPage(null, null, null, e, true);
        else
            fut.onPage(null, metaData, data, null, finished);

        return true;
    }

    /** {@inheritDoc} */
    @Override public void start0() throws IgniteCheckedException {
        super.start0();

        assert cctx.config().getCacheMode() == LOCAL;
    }

    /** {@inheritDoc} */
    @Override public CacheQueryFuture<?> queryDistributed(GridCacheQueryBean qry, Collection<ClusterNode> nodes) {
        assert cctx.config().getCacheMode() == LOCAL;

        throw new IgniteException("Distributed queries are not available for local cache " +
            "(use 'CacheQuery.execute(grid.forLocal())' instead) [cacheName=" + cctx.name() + ']');
    }

    /** {@inheritDoc} */
    @Override public GridCloseableIterator scanQueryDistributed(GridCacheQueryAdapter qry,
        Collection<ClusterNode> nodes) throws IgniteCheckedException {
        assert cctx.isLocal() : cctx.name();

        throw new IgniteException("Distributed scan query are not available for local cache " +
            "(use 'CacheQuery.executeScanQuery(grid.forLocal())' instead) [cacheName=" + cctx.name() + ']');
    }

    /** {@inheritDoc} */
    @Override public void loadPage(long id, GridCacheQueryAdapter<?> qry, Collection<ClusterNode> nodes, boolean all) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public CacheQueryFuture<?> queryFieldsLocal(GridCacheQueryBean qry) {
        assert cctx.config().getCacheMode() == LOCAL;

        if (log.isDebugEnabled())
            log.debug("Executing query on local node: " + qry);

        GridCacheLocalFieldsQueryFuture fut = new GridCacheLocalFieldsQueryFuture(cctx, qry);

        try {
            qry.query().validate();

            fut.execute();
        }
        catch (IgniteCheckedException e) {
            fut.onDone(e);
        }

        return fut;
    }

    /** {@inheritDoc} */
    @Override public CacheQueryFuture<?> queryFieldsDistributed(GridCacheQueryBean qry,
        Collection<ClusterNode> nodes) {
        assert cctx.config().getCacheMode() == LOCAL;

        throw new IgniteException("Distributed queries are not available for local cache " +
            "(use 'CacheQuery.execute(grid.forLocal())' instead) [cacheName=" + cctx.name() + ']');
    }
}
