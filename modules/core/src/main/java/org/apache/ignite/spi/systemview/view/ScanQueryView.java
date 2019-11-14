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
    /** */
    private final UUID nodeId;

    /** */
    private final long qryId;

    /** */
    private final boolean canceled;

    /** */
    private final ScanQueryIterator iter;

    /** */
    public <K, V> ScanQueryView(UUID nodeId, long qryId, boolean canceled,
        IgniteSpiCloseableIterator<IgniteBiTuple<K, V>> iter) {
        this.nodeId = nodeId;
        this.qryId = qryId;
        this.canceled = canceled;
        this.iter = (ScanQueryIterator)iter;
    }

    /** @return . */
    @Order
    public UUID originNodeId() {
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
        return iter.cacheContext().name();
    }

    /** @return . */
    @Order(3)
    public int cacheId() {
        return iter.cacheContext().cacheId();
    }

    /** @return . */
    @Order(4)
    public int cacheGroupId() {
        return iter.cacheContext().groupId();
    }

    /** @return . */
    @Order(5)
    public String cacheGroupName() {
        return iter.cacheContext().group().cacheOrGroupName();
    }

    /** @return . */
    @Order(6)
    public long startTime() {
        return iter.startTime();
    }

    /** @return . */
    @Order(7)
    public long duration() {
        return U.currentTimeMillis() - iter.startTime();
    }

    /** @return . */
    public String filter() {
        IgniteBiPredicate filter = iter.filter();

        return filter == null ? null : toStringSafe(filter);
    }

    /** @return . */
    public int partition() {

        GridDhtLocalPartition part = iter.localPartition();

        if (part == null)
            return -1;

        return part.id();
    }

    /** @return . */
    public boolean local() {
        return iter.local();
    }

    /** @return . */
    public String transformer() {
        IgniteClosure<?, ?> trans = iter.transformer();

        return trans == null ? null : toStringSafe(trans);
    }

    /** @return . */
    public String topology() {
        return toStringSafe(iter.topVer());
    }

    public boolean keepBinary() {
        return iter.keepBinary();
    }

    public UUID subjectId() {
        return iter.subjectId();
    }

    public String taskName() {
        return iter.taskName();
    }
}
