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
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.managers.systemview.walker.ScanQueryViewWalker;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
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
 * @see GridCacheQueryManager.ScanQueryIterator
 * @see ScanQueryView
 */
public class ScanQuerySystemView<K, V> extends AbstractSystemView<ScanQueryView> {
    /** Scan query system view. */
    public static final String SCAN_QRY_SYS_VIEW = metricName("scan", "queries");

    /** Scan query system view description. */
    public static final String SCAN_QRY_SYS_VIEW_DESC = "Scan queries";

    /** Cache data. */
    private Collection<GridCacheContext<K, V>> ctxs;

    /**
     * @param ctxs Cache data.
     */
    public ScanQuerySystemView(Collection<GridCacheContext<K, V>> ctxs) {
        super(SCAN_QRY_SYS_VIEW, SCAN_QRY_SYS_VIEW_DESC, ScanQueryView.class, new ScanQueryViewWalker());

        this.ctxs = ctxs;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        int sz = 0;

        DataIterator iter = new DataIterator();

        while(iter.hasNext())
            sz++;

        return sz;
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<ScanQueryView> iterator() {
        return new DataIterator();
    }

    /** Class to iterate through all {@link GridCacheQueryManager.ScanQueryIterator}. */
    private class DataIterator implements Iterator<ScanQueryView> {
        /** Cache contexts iterator. */
        private final Iterator<GridCacheContext<K, V>> ctxsIter;

        /** Node requests iterator. */
        private Iterator<Map.Entry<UUID, GridCacheQueryManager<K, V>.RequestFutureMap>> nodeQryIter;

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

        /** */
        public DataIterator() {
            this.ctxsIter = ctxs.iterator();
            this.nodeQryIter = Collections.emptyIterator();
            this.qriesIter = Collections.emptyIterator();
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            while (!nextScanIter()) {
                while (!nodeQryIter.hasNext()) {
                    if (!ctxsIter.hasNext())
                        return false;

                    nodeQryIter = ctxsIter.next().queries().queryIterators().entrySet().iterator();
                }

                Map.Entry<UUID, GridCacheQueryManager<K, V>.RequestFutureMap> next = nodeQryIter.next();

                nodeId = next.getKey();

                reqMap = next.getValue();

                qriesIter = next.getValue().entrySet().iterator();
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public ScanQueryView next() {
            return new ScanQueryView(nodeId, qryId, reqMap.isCanceled(qryId), qry);
        }

        /**
         * @return {@code True} if next {@link GridCacheQueryManager.ScanQueryIterator} found.
         */
        private boolean nextScanIter() {
            try {
                while(qriesIter.hasNext()) {
                    Map.Entry<Long, GridFutureAdapter<GridCacheQueryManager.QueryResult<K, V>>> qryRes =
                        qriesIter.next();

                    if (!qryRes.getValue().isDone() || qryRes.getValue().get().type() != SCAN)
                        continue;

                    qryId = qryRes.getKey();

                    qry = qryRes.getValue().get().get();

                    return true;
                }
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }

            return false;
        }
    }
}
