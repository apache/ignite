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

package org.apache.ignite.spi.systemview.view.datastructures;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.datastructures.GridCacheRemovable;
import org.apache.ignite.internal.processors.datastructures.GridCacheSemaphoreImpl;
import org.apache.ignite.spi.systemview.view.SystemView;

/**
 * {@link IgniteSemaphore} representation for a {@link SystemView}.
 *
 * @see Ignite#semaphore(String, int, boolean, boolean)
 */
public class SemaphoreView extends AbstractDataStructureView<GridCacheSemaphoreImpl> {
    /** @param ds Data structure instance. */
    public SemaphoreView(GridCacheRemovable ds) {
        super((GridCacheSemaphoreImpl)ds);
    }

    /**
     * @return Number of permits available.
     * @see IgniteSemaphore#availablePermits()
     */
    @Order(1)
    public long availablePermits() {
        return ds.availablePermits();
    }

    /**
     * @return {@code True} if there may be other threads waiting to acquire the lock.
     * @see IgniteSemaphore#hasQueuedThreads()
     */
    @Order(2)
    public boolean hasQueuedThreads() {
        return ds.hasQueuedThreads();
    }

    /**
     * @return The estimated number of nodes waiting for this lock.
     * @see IgniteSemaphore#getQueueLength()
     */
    @Order(3)
    public int queueLength() {
        return ds.getQueueLength();
    }

    /**
     * @return {@code True} if this semaphore is failover safe.
     * @see IgniteSemaphore#isFailoverSafe()
     */
    @Order(4)
    public boolean failoverSafe() {
        return ds.isFailoverSafe();
    }

    /**
     * @return {@code True} if a node failed on this semaphore and {@link #failoverSafe} flag was set to {@code false},
     * {@code false} otherwise.
     * @see IgniteSemaphore#isBroken()
     */
    @Order(5)
    public boolean broken() {
        return ds.isBroken();
    }
}
