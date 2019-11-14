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

package org.apache.ignite.spi.systemview.view;

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager.ScanQueryIterator;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;

import static org.apache.ignite.internal.util.IgniteUtils.toStringSafe;

/**
 * Scan query iterator representation for a {@link SystemView}.
 */
public class ScanQueryIteratorView<K, V> {
    /** */
    private GridCacheContext<K, V> ctx;

    /** */
    private UUID nodeId;

    /** */
    private long qryId;

    /** */
    private boolean canceled;

    /** */
    private GridFutureAdapter<GridCacheQueryManager.QueryResult<K, V>> qryRes;

    /** */
    public ScanQueryIteratorView(GridCacheContext<K, V> ctx, UUID nodeId, long qryId,
        boolean canceled, GridFutureAdapter<GridCacheQueryManager.QueryResult<K, V>> qryRes) {
        this.ctx = ctx;
        this.nodeId = nodeId;
        this.qryId = qryId;
        this.canceled = canceled;
        this.qryRes = qryRes;
    }

    /** @return . */
    @Order
    public UUID nodeId() {
        return nodeId;
    }

    /** @return . */
    @Order(1)
    public long queryId() {
        return qryId;
    }

    /** @return . */
    public boolean canceled() {
        return canceled;
    }

    /** @return . */
    @Order(2)
    public String cacheName() {
        return ctx.name();
    }

    /** @return . */
    @Order(3)
    public int cacheId() {
        return ctx.cacheId();
    }

    /** @return . */
    @Order(4)
    public int cacheGroupId() {
        return ctx.groupId();
    }

    /** @return . */
    @Order(5)
    public String cacheGroupName() {
        return ctx.group().cacheOrGroupName();
    }

    /** @return . */
    @Order(6)
    public long startTime() {
        ScanQueryIterator iter = iter();

        if (iter == null)
            return -1;

        return iter.startTime();
    }

    /** @return . */
    @Order(7)
    public long duration() {
        ScanQueryIterator iter = iter();

        if (iter == null)
            return -1;

        return U.currentTimeMillis() - iter.startTime();
    }

    /** @return . */
    public String filter() {
        ScanQueryIterator iter = iter();

        if (iter == null)
            return null;

        IgniteBiPredicate filter = iter.filter();

        return filter == null ? null : toStringSafe(filter);
    }

    /** @return . */
    public int partition() {
        ScanQueryIterator iter = iter();

        if (iter == null)
            return -1;

        GridDhtLocalPartition part = iter.localPartition();

        if (part == null)
            return -1;

        return part.id();
    }

    /** @return . */
    public boolean local() {
        ScanQueryIterator iter = iter();

        return iter != null && iter.local();
    }

    /** @return . */
    public String transformer() {
        ScanQueryIterator iter = iter();

        if (iter == null)
            return null;

        IgniteClosure<?, ?> trans = iter.transformer();

        return trans == null ? null : toStringSafe(trans);
    }

    /** @return . */
    public String topology() {
        ScanQueryIterator iter = iter();

        if (iter == null)
            return null;

        return toStringSafe(iter.topVer());
    }

    public boolean keepBinary() {
        ScanQueryIterator iter = iter();

        return iter != null && iter.keepBinary();
    }

    public UUID subjectId() {
        ScanQueryIterator iter = iter();

        if (iter == null)
            return null;

        return iter.subjectId();
    }

    public String taskName() {
        ScanQueryIterator iter = iter();

        if (iter == null)
            return null;

        return iter.taskName();
    }

    /** @return Scan query iterator if exists. */
    private ScanQueryIterator iter() {
        try {
            if (!qryRes.isDone())
                return null;

            GridCacheQueryManager.QueryResult<K, V> result = qryRes.get();

            return (ScanQueryIterator)result.get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }
}
