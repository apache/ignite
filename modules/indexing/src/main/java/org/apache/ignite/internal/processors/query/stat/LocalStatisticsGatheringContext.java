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

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Statistics gathering context.
 */
public class LocalStatisticsGatheringContext {
    /** Remaining partitions */
    private final Set<Integer> remainingParts;

    /**
     *  Complete gathering local partitioned statistics future.
     *  Result:
     *  {@code true} gathering complete (stats from all partitions are gathered),
     *  {@code false} gathering incomplete: one or more partitions not available.
     */
    private final CompletableFuture<Boolean> futGather;

    /** Aggregate local statistic future. */
    private final CompletableFuture<ObjectStatisticsImpl> futAggregate;

    /** Successfully complete status. */
    private boolean completeStatus = true;

    /**
     * Constructor.
     *
     * @param remainingParts Set of partition ids to collect.
     */
    public LocalStatisticsGatheringContext(Set<Integer> remainingParts) {
        this.remainingParts = new HashSet<>(remainingParts);
        this.futGather = new CompletableFuture<>();
        this.futAggregate = new CompletableFuture<>();
    }

    /**
     * Decrement remaining due to successfully processed partition.
     *
     * @param partId Partition id.
     */
    public synchronized void partitionDone(int partId) {
        remainingParts.remove(partId);

        if (remainingParts.isEmpty())
            futGather.complete(completeStatus);
    }

    /**
     * Decrement remaining due to unavailable partition.
     *
     * @param partId Unavailable partition id.
     */
    public synchronized void partitionNotAvailable(int partId) {
        remainingParts.remove(partId);

        completeStatus = false;

        if (remainingParts.isEmpty())
            futGather.complete(completeStatus);
    }

    /**
     *  Complete gathering local partitioned statistics future.
     *  Result:
     *  {@code true} gathering complete (stats from all partitions are gathered),
     *  {@code false} gathering incomplete: one or more partitions not available.
     *
     * @return Gathering future.
     */
    public CompletableFuture<Boolean> futureGather() {
        return futGather;
    }

    /**
     * Complete aggregation local statistics future.
     *
     * @return Aggregate future.
     */
    public CompletableFuture<ObjectStatisticsImpl> futureAggregate() {
        return futAggregate;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(LocalStatisticsGatheringContext.class, this);
    }
}
