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
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager.ScanQueryIterator;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.spi.IgniteSpiCloseableIterator;

import static org.apache.ignite.internal.util.IgniteUtils.toStringSafe;

/**
 * Scan query iterator representation for a {@link SystemView}.
 */
public class ScanQueryView {
    /** Origin node id. */
    private final UUID nodeId;

    /** Local query id. */
    private final long qryId;

    /** Canceled flag. */
    private final boolean canceled;

    /** Scan query iterator. */
    private final ScanQueryIterator iter;

    /**
     * @param nodeId Originating node id.
     * @param qryId Query id.
     * @param canceled {@code True} if query canceled.
     * @param iter Query iterator.
     * @param <K> Key type.
     * @param <V> Value type.
     */
    public <K, V> ScanQueryView(UUID nodeId, long qryId, boolean canceled,
        IgniteSpiCloseableIterator<IgniteBiTuple<K, V>> iter) {
        this.nodeId = nodeId;
        this.qryId = qryId;
        this.canceled = canceled;
        this.iter = (ScanQueryIterator)iter;
    }

    /** @return Origin node id. */
    @Order
    public UUID originNodeId() {
        return nodeId;
    }

    /** @return Local query id. */
    @Order(1)
    public long queryId() {
        return qryId;
    }

    /** @return {@code True} if query canceled. */
    public boolean canceled() {
        return canceled;
    }

    /** @return Cache name. */
    @Order(2)
    public String cacheName() {
        return iter.cacheContext().name();
    }

    /** @return Cache id. */
    @Order(3)
    public int cacheId() {
        return iter.cacheContext().cacheId();
    }

    /** @return Cache group id. */
    @Order(4)
    public int cacheGroupId() {
        return iter.cacheContext().groupId();
    }

    /** @return Cache group name. */
    @Order(5)
    public String cacheGroupName() {
        return iter.cacheContext().group().cacheOrGroupName();
    }

    /** @return Start time. */
    @Order(6)
    public long startTime() {
        return iter.startTime();
    }

    /** @return Query duration. */
    @Order(7)
    public long duration() {
        return U.currentTimeMillis() - iter.startTime();
    }

    /** @return Filter. */
    public String filter() {
        IgniteBiPredicate filter = iter.filter();

        return filter == null ? null : toStringSafe(filter);
    }

    /** @return Cache partition. */
    public int partition() {
        GridDhtLocalPartition part = iter.localPartition();

        if (part == null)
            return -1;

        return part.id();
    }

    /** @return {@code True} if query local. */
    public boolean local() {
        return iter.local();
    }

    /** @return Transformer. */
    public String transformer() {
        IgniteClosure<?, ?> trans = iter.transformer();

        return trans == null ? null : toStringSafe(trans);
    }

    /** @return Topology. */
    public String topology() {
        return toStringSafe(iter.topVer());
    }

    /** @return Keep binary flag. */
    public boolean keepBinary() {
        return iter.keepBinary();
    }

    /** @return Subject id. */
    public UUID subjectId() {
        return iter.subjectId();
    }

    /** @return Task name. */
    public String taskName() {
        return iter.taskName();
    }

    /** @return Page size. */
    public int pageSize() {
        return iter.pageSize();
    }
}
