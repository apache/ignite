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
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Statistics gathering context.
 */
public class LocalStatisticsGatheringContext {
    /** Additional steps count: prepare to prevent cancellation if some service tasks
     * (save obsolescense or drop table).
     */
    private static final int ADDITIONAL_STEPS = 1;

    /** Force recollect flag. */
    private final boolean forceRecollect;

    /** Table to process. */
    private final GridH2Table tbl;

    /** Statistics configuration to use. */
    private final StatisticsObjectConfiguration cfg;

    /** Remaining partitions */
    private final Set<Integer> remainingParts;

    /** All partitions for aggregate. */
    private final Set<Integer> allParts;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Future with success status as a result. */
    private CompletableFuture<Void> future;

    /** Context cancelled flag. */
    private volatile boolean cancelled;

    /**
     * Constructor.
     *
     * @param forceRecollect Force recollect flag.
     * @param tbl Table to process.
     * @param cfg Statistics configuration to use.
     * @param remainingParts Set of partition ids to collect.
     */
    public LocalStatisticsGatheringContext(
        boolean forceRecollect,
        GridH2Table tbl,
        StatisticsObjectConfiguration cfg,
        Set<Integer> remainingParts,
        AffinityTopologyVersion topVer
    ) {
        this.forceRecollect = forceRecollect;
        this.tbl = tbl;
        this.cfg = cfg;
        this.remainingParts = new HashSet<>(remainingParts);
        this.allParts = (forceRecollect) ? null : new HashSet<>(remainingParts);
        this.topVer = topVer;
        this.future = new CompletableFuture<>();
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
    public GridH2Table table() {
        return tbl;
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
     * @return
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
     * @return {@code true} if no more partitions left, {@code false} - otherwise.
     */
    public synchronized boolean partitionNotAvailable(int partId) {
        remainingParts.remove(partId);

        cancel();

        if (remainingParts.isEmpty()) {
            future.cancel(true);

            return true;
        }

        return false;
    }

    public void cancel() {
        cancelled = true;
    }

    public boolean cancelled() {
        return cancelled;
    }

    /**
     * @return
     */
    public CompletableFuture<Void> future() {
        return future;
    }

    /**
     * @param future
     */
    public void future(CompletableFuture<Void> future) {
        this.future = future;
    }

    /**
     * @return
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(LocalStatisticsGatheringContext.class, this);
    }
}
