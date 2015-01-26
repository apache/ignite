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

import org.apache.ignite.*;

/**
 * This interface provides a rich API for working with distributedly cached atomic long value.
 * <p>
 * <h1 class="header">Functionality</h1>
 * Distributed atomic long includes the following main functionality:
 * <ul>
 * <li>
 * Method {@link #get()} synchronously gets current value of atomic long.
 * </li>
 * <li>
 * Various {@code get..(..)} methods synchronously get current value of atomic long
 * and increase or decrease value of atomic long.
 * </li>
 * <li>
 * Method {@link #addAndGet(long l)} synchronously sums {@code l} with current value of atomic long
 * and returns result.
 * </li>
 * <li>
 * Method {@link #incrementAndGet()} synchronously increases value of atomic long and returns result.
 * </li>
 * <li>
 * Method {@link #decrementAndGet()} synchronously decreases value of atomic long and returns result.
 * </li>
 * <li>
 * Method {@link #getAndSet(long l)} synchronously gets current value of atomic long and sets {@code l}
 * as value of atomic long.
 * </li>
 * </ul>
 * All previously described methods have asynchronous analogs.
 * <ul>
 * <li>
 * Method {@link #name()} gets name of atomic long.
 * </li>
 * </ul>
 * <p>
 * <h1 class="header">Creating Distributed Atomic Long</h1>
 * Instance of distributed atomic long can be created by calling the following method:
 * <ul>
 *     <li>{@link org.apache.ignite.cache.datastructures.CacheDataStructures#atomicLong(String, long, boolean)}</li>
 * </ul>
 * @see org.apache.ignite.cache.datastructures.CacheDataStructures#atomicLong(String, long, boolean)
 * @see org.apache.ignite.cache.datastructures.CacheDataStructures#removeAtomicLong(String)
 */
public interface IgniteAtomicLong {
    /**
     * Name of atomic long.
     *
     * @return Name of atomic long.
     */
    public String name();

    /**
     * Gets current value of atomic long.
     *
     * @return Current value of atomic long.
     * @throws IgniteCheckedException If operation failed.
     */
    public long get() throws IgniteCheckedException;

    /**
     * Increments and gets current value of atomic long.
     *
     * @return Value.
     * @throws IgniteCheckedException If operation failed.
     */
    public long incrementAndGet() throws IgniteCheckedException;

    /**
     * Gets and increments current value of atomic long.
     *
     * @return Value.
     * @throws IgniteCheckedException If operation failed.
     */
    public long getAndIncrement() throws IgniteCheckedException;

    /**
     * Adds {@code l} and gets current value of atomic long.
     *
     * @param l Number which will be added.
     * @return Value.
     * @throws IgniteCheckedException If operation failed.
     */
    public long addAndGet(long l) throws IgniteCheckedException;

    /**
     * Gets current value of atomic long and adds {@code l}.
     *
     * @param l Number which will be added.
     * @return Value.
     * @throws IgniteCheckedException If operation failed.
     */
    public long getAndAdd(long l) throws IgniteCheckedException;

    /**
     * Decrements and gets current value of atomic long.
     *
     * @return Value.
     * @throws IgniteCheckedException If operation failed.
     */
    public long decrementAndGet() throws IgniteCheckedException;

    /**
     * Gets and decrements current value of atomic long.
     *
     * @return Value.
     * @throws IgniteCheckedException If operation failed.
     */
    public long getAndDecrement() throws IgniteCheckedException;

    /**
     * Gets current value of atomic long and sets new value {@code l} of atomic long.
     *
     * @param l New value of atomic long.
     * @return Value.
     * @throws IgniteCheckedException If operation failed.
     */
    public long getAndSet(long l) throws IgniteCheckedException;

    /**
     * Atomically compares current value to the expected value, and if they are equal, sets current value
     * to new value.
     *
     * @param expVal Expected atomic long's value.
     * @param newVal New atomic long's value to set if current value equal to expected value.
     * @return {@code True} if comparison succeeded, {@code false} otherwise.
     * @throws IgniteCheckedException If failed.
     */
    public boolean compareAndSet(long expVal, long newVal) throws IgniteCheckedException;

    /**
     * Gets status of atomic.
     *
     * @return {@code true} if atomic was removed from cache, {@code false} in other case.
     */
    public boolean removed();
}
