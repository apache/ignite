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

package org.apache.ignite.internal.processors.query.stat;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Statistics gathering context.
 */
public class LocalStatisticsGatheringContext {
    /** Force recollect flag. */
    private final boolean forceRecollect;

    /** Table to process. */
    private final GridQueryTypeDescriptor tbl;

    /** Table cache context. */
    private final GridCacheContextInfo<?, ?> cctxInfo;

    /** Statistics configuration to use. */
    private final StatisticsObjectConfiguration cfg;

    /** Remaining partitions */
    private final Set<Integer> remainingParts;

    /** All partitions for aggregate. */
    private final Set<Integer> allParts;

    /** Topology version. */
    private final AffinityTopologyVersion topVer;

    /** Future with success status as a result. */
    private final CompletableFuture<Void> future;

    /** Context cancelled flag. */
    private volatile boolean cancelled;

    /** Binary signed or unsiged compare mode. */
    private final boolean isBinaryUnsigned;

    /**
     * Constructor.
     *
     * @param forceRecollect Force recollect flag.
     * @param tbl Table to process.
     * @param cctxInfo Cache context info;
     * @param cfg Statistics configuration to use.
     * @param remainingParts Set of partition ids to collect.
     * @param isBinaryUnsigned Binary signed or unsiged compare mode.
     */
    public LocalStatisticsGatheringContext(
        boolean forceRecollect,
        GridQueryTypeDescriptor tbl,
        GridCacheContextInfo<?, ?> cctxInfo,
        StatisticsObjectConfiguration cfg,
        Set<Integer> remainingParts,
        AffinityTopologyVersion topVer,
        boolean isBinaryUnsigned
    ) {
        this.forceRecollect = forceRecollect;
        this.tbl = tbl;
        this.cctxInfo = cctxInfo;
        this.cfg = cfg;
        this.remainingParts = new HashSet<>(remainingParts);
        this.allParts = (forceRecollect) ? null : new HashSet<>(remainingParts);
        this.topVer = topVer;
        this.future = new CompletableFuture<>();
        this.isBinaryUnsigned = isBinaryUnsigned;
    }

    /**
     * @return Force recollect flag.
     */
    public boolean forceRecollect() {
        return forceRecollect;
    }

    /**
     * @return Table to process.
     */
    public GridQueryTypeDescriptor table() {
        return tbl;
    }

    /**
     * @return Cache context of processing table.
     */
    public GridCacheContextInfo<?, ?> cacheContextInfo() {
        return cctxInfo;
    }

    /**
     * @return Statistics configuration to collect with.
     */
    public StatisticsObjectConfiguration configuration() {
        return cfg;
    }

    /**
     * Decrement remaining partitions due to successfully processed partition.
     *
     * @param partId Partition id.
     * @return {@code true} if no more partitions left, {@code false} - otherwise.
     */
    public synchronized boolean partitionDone(int partId) {
        remainingParts.remove(partId);
        return remainingParts.isEmpty();
    }

    /**
     * @return Set of remaining partitions.
     */
    public synchronized Set<Integer> remainingParts() {
        return new HashSet<>(remainingParts);
    }

    /**
     * @return All primary partitions or {@code null} if there was just byObsolescence recollection.
     */
    public Set<Integer> allParts() {
        return allParts;
    }

    /**
     * Decrement remaining partitions due to unavailable partition.
     *
     * @param partId Unavailable partition id.
     */
    public synchronized void partitionNotAvailable(int partId) {
        remainingParts.remove(partId);

        cancel();

        if (remainingParts.isEmpty()) {
            future.cancel(true);

            return;
        }

        return;
    }

    /**
     * Cancel gathering.
     */
    public void cancel() {
        cancelled = true;
    }

    /**
     * @return Cancelled flag.
     */
    public boolean cancelled() {
        return cancelled;
    }

    /**
     * @return Gathering completable future.
     */
    public CompletableFuture<Void> future() {
        return future;
    }

    /**
     * @return Gathering topology version or {@code null} if it's just an obsolescence processing.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Binary signed or unsigned compare mode.
     */
    public boolean isBinaryUnsigned() {
        return isBinaryUnsigned;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(LocalStatisticsGatheringContext.class, this);
    }
}
