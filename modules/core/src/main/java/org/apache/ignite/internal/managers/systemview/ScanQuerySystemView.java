/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.managers.systemview;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.managers.systemview.walker.ScanQueryViewWalker;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager.ScanQueryIterator;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.IgniteSpiCloseableIterator;
import org.apache.ignite.spi.systemview.view.ScanQueryView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SCAN;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * {@link SystemView} implementation providing data about cache queries.
 *
 * @see GridCacheQueryManager#queryIterators()
 * @see ScanQueryIterator
 * @see ScanQueryView
 */
public class ScanQuerySystemView<K, V> extends AbstractSystemView<ScanQueryView> {
    /** Scan query system view. */
    public static final String SCAN_QRY_SYS_VIEW = metricName("scan", "queries");

    /** Scan query system view description. */
    public static final String SCAN_QRY_SYS_VIEW_DESC = "Scan queries";

    /** Cache data. */
    private final Collection<GridCacheContext<K, V>> cctxs;

    /**
     * @param cctxs Cache data.
     */
    public ScanQuerySystemView(Collection<GridCacheContext<K, V>> cctxs) {
        super(SCAN_QRY_SYS_VIEW, SCAN_QRY_SYS_VIEW_DESC, new ScanQueryViewWalker());

        this.cctxs = cctxs;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        int sz = 0;

        QueryDataIterator iter = new QueryDataIterator();

        //Intentionally don't call `next` method to prevent the creation of unused objects and reduce GC pressure.
        while (iter.hasNext())
            sz++;

        return sz;
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<ScanQueryView> iterator() {
        return new QueryDataIterator();
    }

    /** Class to iterate through all {@link GridCacheQueryManager.ScanQueryIterator}. */
    private class QueryDataIterator implements Iterator<ScanQueryView> {
        /** Cache contexts iterator. */
        private final Iterator<GridCacheContext<K, V>> cctxsIter;

        /** Current cache context. */
        private GridCacheContext<K, V> cctx;

        /** Node requests iterator. */
        private Iterator<Map.Entry<UUID, GridCacheQueryManager<K, V>.RequestFutureMap>> nodeQryIter;

        /** Local query iterator. */
        private Iterator<GridCacheQueryManager.ScanQueryIterator> localQryIter;

        /** Current node id. */
        private UUID nodeId;

        /** Current requests map. */
        private GridCacheQueryManager<K, V>.RequestFutureMap reqMap;

        /** Request results iterator. */
        private Iterator<Map.Entry<Long, GridFutureAdapter<GridCacheQueryManager.QueryResult<K, V>>>> qriesIter;

        /** Current query id. */
        private long qryId;

        /** Current scan iterator. */
        private IgniteSpiCloseableIterator<IgniteBiTuple<K, V>> qry;

        /** {@code True} if {@link #hasNext()} was executed before the {@link #next()}. */
        private boolean hasNextExec;

        /** */
        public QueryDataIterator() {
            cctxsIter = cctxs.iterator();
            nodeQryIter = Collections.emptyIterator();
            qriesIter = Collections.emptyIterator();
            localQryIter = Collections.emptyIterator();
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            hasNextExec = true;

            while (!nextScanIter()) {
                while (!nodeQryIter.hasNext() && !localQryIter.hasNext()) {
                    if (!cctxsIter.hasNext())
                        return false;

                    cctx = cctxsIter.next();

                    GridCacheQueryManager<K, V> qryMgr = cctx.queries();

                    nodeQryIter = qryMgr.queryIterators().entrySet().iterator();

                    localQryIter = qryMgr.localQueryIterators().iterator();
                }

                if (nodeQryIter.hasNext()) {
                    Map.Entry<UUID, GridCacheQueryManager<K, V>.RequestFutureMap> next = nodeQryIter.next();

                    nodeId = next.getKey();

                    reqMap = next.getValue();

                    qriesIter = next.getValue().entrySet().iterator();
                }
                else {
                    nodeId = cctx.localNodeId();

                    reqMap = null;

                    qriesIter = null;
                }
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public ScanQueryView next() {
            if (!hasNextExec && !hasNext())
                throw new NoSuchElementException("No more elements.");

            hasNextExec = false;

            return new ScanQueryView(nodeId, qryId, reqMap != null && reqMap.isCanceled(qryId), qry);
        }

        /**
         * @return {@code True} if next {@link GridCacheQueryManager.ScanQueryIterator} found.
         */
        private boolean nextScanIter() {
            try {
                while (qriesIter != null && qriesIter.hasNext()) {
                    Map.Entry<Long, GridFutureAdapter<GridCacheQueryManager.QueryResult<K, V>>> qryRes =
                        qriesIter.next();

                    if (!qryRes.getValue().isDone() || qryRes.getValue().get().type() != SCAN)
                        continue;

                    qryId = qryRes.getKey();

                    qry = qryRes.getValue().get().get();

                    return true;
                }

                qriesIter = null;

                reqMap = null;

                if (!localQryIter.hasNext())
                    return false;

                qryId = 0;

                qry = localQryIter.next();

                nodeId = cctx.localNodeId();

                return true;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }
}
