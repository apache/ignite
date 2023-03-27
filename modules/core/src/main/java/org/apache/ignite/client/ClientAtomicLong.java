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

package org.apache.ignite.client;

import java.io.Closeable;
import org.apache.ignite.IgniteException;

/**
 * Distributed atomic long API.
 * <p>
 * <h1 class="header">Functionality</h1>
 * Distributed atomic long includes the following main functionality:
 * <ul>
 * <li>
 * Method {@link #get()} gets current value of atomic long.
 * </li>
 * <li>
 * Various {@code get..(..)} methods get current value of atomic long
 * and increase or decrease value of atomic long.
 * </li>
 * <li>
 * Method {@link #addAndGet(long l)} sums {@code l} with current value of atomic long
 * and returns result.
 * </li>
 * <li>
 * Method {@link #incrementAndGet()} increases value of atomic long and returns result.
 * </li>
 * <li>
 * Method {@link #decrementAndGet()} decreases value of atomic long and returns result.
 * </li>
 * <li>
 * Method {@link #getAndSet(long l)} gets current value of atomic long and sets {@code l}
 * as value of atomic long.
 * </li>
 * <li>
 * Method {@link #name()} gets name of atomic long.
 * </li>
 * </ul>
 * <p>
 * <h1 class="header">Creating Distributed Atomic Long</h1>
 * Instance of distributed atomic long can be created by calling the following method:
 * <ul>
 *     <li>{@link IgniteClient#atomicLong(String, long, boolean)}</li>
 * </ul>
 * @see IgniteClient#atomicLong(String, long, boolean)
 */
public interface ClientAtomicLong extends Closeable {
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
     */
    public long get() throws IgniteException;

    /**
     * Increments and gets current value of atomic long.
     *
     * @return Value.
     */
    public long incrementAndGet() throws IgniteException;

    /**
     * Gets and increments current value of atomic long.
     *
     * @return Value.
     */
    public long getAndIncrement() throws IgniteException;

    /**
     * Adds {@code l} and gets current value of atomic long.
     *
     * @param l Number which will be added.
     * @return Value.
     */
    public long addAndGet(long l) throws IgniteException;

    /**
     * Gets current value of atomic long and adds {@code l}.
     *
     * @param l Number which will be added.
     * @return Value.
     */
    public long getAndAdd(long l) throws IgniteException;

    /**
     * Decrements and gets current value of atomic long.
     *
     * @return Value.
     */
    public long decrementAndGet() throws IgniteException;

    /**
     * Gets and decrements current value of atomic long.
     *
     * @return Value.
     */
    public long getAndDecrement() throws IgniteException;

    /**
     * Gets current value of atomic long and sets new value {@code l} of atomic long.
     *
     * @param l New value of atomic long.
     * @return Value.
     */
    public long getAndSet(long l) throws IgniteException;

    /**
     * Atomically compares current value to the expected value, and if they are equal, sets current value
     * to new value.
     *
     * @param expVal Expected atomic long's value.
     * @param newVal New atomic long's value to set if current value equal to expected value.
     * @return {@code True} if comparison succeeded, {@code false} otherwise.
     */
    public boolean compareAndSet(long expVal, long newVal) throws IgniteException;

    /**
     * Gets status of atomic.
     *
     * @return {@code true} if atomic was removed from cache, {@code false} in other case.
     */
    public boolean removed();

    /**
     * Removes this atomic long.
     */
    @Override public void close();
}
