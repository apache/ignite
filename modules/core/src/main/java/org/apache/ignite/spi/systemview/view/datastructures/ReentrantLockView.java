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
import org.apache.ignite.IgniteLock;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.datastructures.GridCacheLockImpl;
import org.apache.ignite.internal.processors.datastructures.GridCacheRemovable;
import org.apache.ignite.spi.systemview.view.SystemView;

/**
 * {@link IgniteLock} representation for a {@link SystemView}.
 *
 * @see Ignite#reentrantLock(String, boolean, boolean, boolean)
 */
public class ReentrantLockView extends AbstractDataStructureView<GridCacheLockImpl> {
    /** @param ds Data structure instance. */
    public ReentrantLockView(GridCacheRemovable ds) {
        super((GridCacheLockImpl)ds);
    }

    /**
     * @return {@code True} if locked.
     * @see IgniteLock#isLocked()
     */
    @Order(1)
    public boolean locked() {
        return ds.isLocked();
    }

    /**
     * @return {@code True} if there may be other threads waiting to acquire the lock.
     * @see IgniteLock#hasQueuedThreads()
     */
    @Order(2)
    public boolean hasQueuedThreads() {
        return ds.hasQueuedThreads();
    }

    /**
     * @return {@code True} if this semaphore is failover safe.
     * @see IgniteLock#isFailoverSafe()
     */
    @Order(3)
    public boolean failoverSafe() {
        return ds.isFailoverSafe();
    }

    /**
     * @return {@code True} if this lock is fair.
     * @see IgniteLock#isFair()
     */
    @Order(4)
    public boolean fair() {
        return ds.isFair();
    }

    /**
     * @return {@code True} if a node failed on this semaphore and {@link #failoverSafe} flag was set to {@code false},
     * {@code false} otherwise.
     * @see IgniteLock#isBroken()
     */
    @Order(5)
    public boolean broken() {
        return ds.isBroken();
    }
}
