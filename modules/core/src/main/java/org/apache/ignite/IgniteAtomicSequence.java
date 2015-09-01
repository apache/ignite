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

import java.io.Closeable;

/**
 * This interface provides a rich API for working with distributed atomic sequence.
 * <p>
 * <h1 class="header">Functionality</h1>
 * Distributed atomic sequence includes the following main functionality:
 * <ul>
 * <li>
 * Method {@link #get()} synchronously gets current value from atomic sequence.
 * </li>
 * <li>
 * Various {@code get..(..)} methods synchronously get current value from atomic sequence
 * and increase atomic sequences value.
 * </li>
 * <li>
 * Various {@code add..(..)} {@code increment(..)} methods synchronously increase atomic sequences value
 * and return increased value.
 * </li>
 * </ul>
 * All previously described methods have asynchronous analogs.
 * <ul>
 * <li>
 * Method {@link #batchSize(int size)} sets batch size of current atomic sequence.
 * </li>
 * <li>
 * Method {@link #batchSize()} gets current batch size of atomic sequence.
 * </li>
 * <li>
 * Method {@link #name()} gets name of atomic sequence.
 * </li>
 * </ul>
 * <h1 class="header">Creating Distributed Atomic Sequence</h1>
 * Instance of distributed atomic sequence can be created by calling the following method:
 * <ul>
 *     <li>{@link Ignite#atomicSequence(String, long, boolean)}</li>
 * </ul>
 * @see Ignite#atomicSequence(String, long, boolean)
 */
public interface IgniteAtomicSequence extends Closeable {
    /**
     * Name of atomic sequence.
     *
     * @return Name of atomic sequence.
     */
    public String name();

    /**
     * Gets current value of atomic sequence.
     *
     * @return Value of atomic sequence.
     * @throws IgniteException If operation failed.
     */
    public long get() throws IgniteException;

    /**
     * Increments and returns the value of atomic sequence.
     *
     * @return Value of atomic sequence after increment.
     * @throws IgniteException If operation failed.
     */
    public long incrementAndGet() throws IgniteException;

    /**
     * Gets and increments current value of atomic sequence.
     *
     * @return Value of atomic sequence before increment.
     * @throws IgniteException If operation failed.
     */
    public long getAndIncrement() throws IgniteException;

    /**
     * Adds {@code l} elements to atomic sequence and gets value of atomic sequence.
     *
     * @param l Number of added elements.
     * @return Value of atomic sequence.
     * @throws IgniteException If operation failed.
     */
    public long addAndGet(long l) throws IgniteException;

    /**
     * Gets current value of atomic sequence and adds {@code l} elements.
     *
     * @param l Number of added elements.
     * @return Value of atomic sequence.
     * @throws IgniteException If operation failed.
     */
    public long getAndAdd(long l) throws IgniteException;

    /**
     * Gets local batch size for this atomic sequence.
     *
     * @return Sequence batch size.
     */
    public int batchSize();

    /**
     * Sets local batch size for atomic sequence.
     *
     * @param size Sequence batch size. Must be more then 0.
     */
    public void batchSize(int size);

    /**
     * Gets status of atomic sequence.
     *
     * @return {@code true} if atomic sequence was removed from cache, {@code false} otherwise.
     */
    public boolean removed();

    /**
     * Removes this atomic sequence.
     *
     * @throws IgniteException If operation failed.
     */
    @Override public void close();
}