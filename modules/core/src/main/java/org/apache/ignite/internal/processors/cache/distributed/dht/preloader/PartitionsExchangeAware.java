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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;

/**
 * Interface which allows to subscribe a component for partition map exchange events
 * (via {@link GridCachePartitionExchangeManager#registerExchangeAwareComponent(PartitionsExchangeAware)}).
 * Heavy computations shouldn't be performed in listener methods: aware components will be notified
 * synchronously from exchange thread.
 * Runtime exceptions thrown by listener methods will trigger failure handler (as per exchange thread is critical).
 * Please ensure that your implementation will never throw an exception if you subscribe to exchange events for
 * non-system-critical activities.
 */
public interface PartitionsExchangeAware {
    /**
     * Callback from exchange process initialization; called before topology is locked.
     *
     * @param fut Partition map exchange future.
     */
    public default void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
        // No-op.
    }

    /**
     * Callback from exchange process initialization; called after topology is locked.
     * Guarantees that no more data updates will be performed on local node until exchange process is completed.
     *
     * @param fut Partition map exchange future.
     */
    public default void onInitAfterTopologyLock(GridDhtPartitionsExchangeFuture fut) {
        // No-op.
    }

    /**
     * Callback from exchange process completion; called before topology is unlocked.
     * Guarantees that no updates were performed on local node since exchange process started.
     *
     * @param fut Partition map exchange future.
     */
    public default void onDoneBeforeTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
        // No-op.
    }

    /**
     * Callback from exchange process completion; called after topology is unlocked.
     *
     * @param fut Partition map exchange future.
     */
    public default void onDoneAfterTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
        // No-op.
    }
}
