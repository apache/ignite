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

package org.apache.ignite;

import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;

import java.util.concurrent.*;
import java.util.concurrent.locks.*;

/**
 * Lock associated with some cache keys.
 */
public interface CacheLock extends Lock {
    /**
     * Checks if any node owns a lock for the keys associated this lock.
     * <p>
     * This is a local in-VM operation and does not involve any network trips
     * or access to persistent storage in any way.
     *
     * @return {@code True} if lock is owned by some node.
     * @see {@link IgniteCache#isLocked(Object)}
     */
    public boolean isLocked();

    /**
     * Checks if current thread owns a lock the keys associated this lock.
     * <p>
     * This is a local in-VM operation and does not involve any network trips
     * or access to persistent storage in any way.
     *
     * @return {@code True} if key is locked by current thread.
     * @see {@link IgniteCache#isLockedByThread(Object)}
     */
    public boolean isLockedByThread();

    /**
     * Asynchronously acquires lock on a cached object with given keys associated this lock.
     * <h2 class="header">Transactions</h2>
     * Locks are not transactional and should not be used from within transactions. If you do
     * need explicit locking within transaction, then you should use
     * {@link IgniteTxConcurrency#PESSIMISTIC} concurrency control for transaction
     * which will acquire explicit locks for relevant cache operations.
     * <h2 class="header">Cache Flags</h2>
     * This method is not available if any of the following flags are set on projection:
     * {@link GridCacheFlag#LOCAL}, {@link GridCacheFlag#READ}.
     *
     * @return Future for the lock operation. The future will return {@code true}.
     */
    public IgniteFuture<Boolean> lockAsync();

    /**
     * Asynchronously acquires lock on a cached object with given keys associated this lock.
     * <h2 class="header">Transactions</h2>
     * Locks are not transactional and should not be used from within transactions. If you do
     * need explicit locking within transaction, then you should use
     * {@link IgniteTxConcurrency#PESSIMISTIC} concurrency control for transaction
     * which will acquire explicit locks for relevant cache operations.
     * <h2 class="header">Cache Flags</h2>
     * This method is not available if any of the following flags are set on projection:
     * {@link GridCacheFlag#LOCAL}, {@link GridCacheFlag#READ}.
     *
     * @param timeout Timeout in milliseconds to wait for lock to be acquired
     *      ({@code '0'} for no expiration, {@code -1} for immediate failure if
     *      lock cannot be acquired immediately).
     * @param unit the time unit of the {@code timeout} argument.
     * @return Future for the lock operation. The future will return {@code true} whenever locks are acquired before
     *      timeout is expired, {@code false} otherwise.
     */
    public IgniteFuture<Boolean> lockAsync(long timeout, TimeUnit unit);
}
