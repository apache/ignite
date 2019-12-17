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

package org.apache.ignite.internal.processors.query.calcite.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.lang.GridCursor;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UNDEFINED_CACHE_ID;

/**
 *
 */
public class TableScanIterator<T> extends GridCloseableIteratorAdapter<T> {
    private final int cacheId;
    private final Iterator<GridDhtLocalPartition> parts;
    private final Function<CacheDataRow, T> typeWrapper;
    private final Predicate<CacheDataRow> typeFilter;

    /**
     *
     */
    private GridCursor<? extends CacheDataRow> cur;
    /**
     *
     */
    private GridDhtLocalPartition curPart;

    /**
     *
     */
    private T next;

    public TableScanIterator(int cacheId, Iterator<GridDhtLocalPartition> parts, Function<CacheDataRow, T> typeWrapper,
        Predicate<CacheDataRow> typeFilter) {
        this.cacheId = cacheId;
        this.parts = parts;
        this.typeWrapper = typeWrapper;
        this.typeFilter = typeFilter;
    }

    @Override
    protected T onNext() {
        if (next == null)
            throw new NoSuchElementException();

        T next = this.next;

        this.next = null;

        return next;
    }

    @Override
    protected boolean onHasNext() throws IgniteCheckedException {
        if (next != null)
            return true;

        while (true) {
            if (cur == null) {
                if (parts.hasNext()) {
                    GridDhtLocalPartition part = parts.next();

                    if (!reservePartition(part))
                        throw new IgniteSQLException("Failed to reserve partition, please retry on stable topology.");

                    IgniteCacheOffheapManager.CacheDataStore ds = part.dataStore();

                    cur = cacheId == UNDEFINED_CACHE_ID ? ds.cursor() : ds.cursor(cacheId, false);
                } else
                    break;
            }

            if (cur.next()) {
                CacheDataRow row = cur.get();

                if (!typeFilter.test(row))
                    continue;

                next = typeWrapper.apply(row);

                break;
            } else {
                cur = null;

                releaseCurrentPartition();
            }
        }

        return next != null;
    }

    /**
     *
     */
    private void releaseCurrentPartition() {
        GridDhtLocalPartition p = curPart;

        assert p != null;

        curPart = null;

        p.release();
    }

    /**
     *
     */
    private boolean reservePartition(GridDhtLocalPartition p) {
        if (p != null && p.reserve()) {
            curPart = p;

            return true;
        }

        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void onClose() {
        if (curPart != null)
            releaseCurrentPartition();
    }
}
