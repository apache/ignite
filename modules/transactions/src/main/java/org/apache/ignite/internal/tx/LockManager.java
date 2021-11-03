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

package org.apache.ignite.internal.tx;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Lock manager allows to acquire locks in shared and exclusive mode and supports deadlock prevention by timestamp ordering.
 */
public interface LockManager {
    /**
     * @param key       The key.
     * @param timestamp The timestamp.
     * @return The future.
     * @throws LockException When a lock can't be taken due to possible deadlock.
     */
    public CompletableFuture<Void> tryAcquire(Object key, Timestamp timestamp) throws LockException;

    /**
     * @param key       The key.
     * @param timestamp The timestamp.
     * @throws LockException If the unlock operation is invalid.
     */
    public void tryRelease(Object key, Timestamp timestamp) throws LockException;

    /**
     * @param key       The key.
     * @param timestamp The timestamp.
     * @return The future.
     * @throws LockException When a lock can't be taken due to possible deadlock.
     */
    public CompletableFuture<Void> tryAcquireShared(Object key, Timestamp timestamp) throws LockException;

    /**
     * @param key       The key.
     * @param timestamp The timestamp.
     * @throws LockException If the unlock operation is invalid.
     */
    public void tryReleaseShared(Object key, Timestamp timestamp) throws LockException;

    /**
     * @param key The key.
     * @return The waiters queue.
     */
    public Collection<Timestamp> queue(Object key);

    /**
     * @param key       The key.
     * @param timestamp The timestamp.
     * @return The waiter.
     */
    public Waiter waiter(Object key, Timestamp timestamp);
}
