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

package org.apache.ignite.cache;

import org.apache.ignite.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.locks.*;

/**
 * Lock associated with some cache keys.
 */
public interface CacheLock extends Lock {
    /**
     * Checks if any node holds lock on at least one key associated with this {@code CacheLock}.
     * <p>
     * This is a local in-VM operation and does not involve any network trips
     * or access to persistent storage in any way.
     *
     * @return {@code True} if lock is owned by some node.
     * @see {@link IgniteCache#isLocked(Object)}
     */
    public boolean isLocked();

    /**
     * Checks if current thread holds lock on at least one key associated with this {@code CacheLock}.
     * <p>
     * This is a local in-VM operation and does not involve any network trips
     * or access to persistent storage in any way.
     *
     * @return {@code True} if key is locked by current thread.
     * @see {@link IgniteCache#isLockedByThread(Object)}
     */
    public boolean isLockedByThread();

    /**
     * The method is not supported, {@link UnsupportedOperationException} will be thrown.
     *
     * @return This method is not supported, {@link UnsupportedOperationException} will be thrown.
     */
    @NotNull @Override public Condition newCondition();
}
