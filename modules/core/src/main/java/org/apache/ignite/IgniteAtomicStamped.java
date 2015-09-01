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
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * This interface provides a rich API for working with distributed atomic stamped value.
 * <p>
 * <h1 class="header">Functionality</h1>
 * Distributed atomic stamped includes the following main functionality:
 * <ul>
 * <li>
 * Method {@link #get()} synchronously gets both value and stamp of atomic.
 * </li>
 * <li>
 * Method {@link #value()} synchronously gets current value of atomic.
 * </li>
 * <li>
 * Method {@link #stamp()} synchronously gets current stamp of atomic.
 * </li>
 * <li>
 * Method {@link #set(Object, Object)} synchronously and unconditionally sets the value
 * and the stamp in the atomic.
 * </li>
 * <li>
 * Methods {@code compareAndSet(...)} synchronously and conditionally set the value
 * and the stamp in the atomic.
 * </li>
 * </ul>
 * All previously described methods have asynchronous analogs.
 * <ul>
 * <li>
 * Method {@link #name()} gets name of atomic stamped.
 * </li>
 * </ul>
 * <h1 class="header">Creating Distributed Atomic Stamped</h1>
 * Instance of distributed atomic stamped can be created by calling the following method:
 * <ul>
 *     <li>{@link Ignite#atomicLong(String, long, boolean)}</li>
 * </ul>
 * @see Ignite#atomicStamped(String, Object, Object, boolean)
 */
public interface IgniteAtomicStamped<T, S> extends Closeable {
    /**
     * Name of atomic stamped.
     *
     * @return Name of atomic stamped.
     */
    public String name();

    /**
     * Gets both current value and current stamp of atomic stamped.
     *
     * @return both current value and current stamp of atomic stamped.
     * @throws IgniteException If operation failed.
     */
    public IgniteBiTuple<T, S> get() throws IgniteException;

    /**
     * Unconditionally sets the value and the stamp.
     *
     * @param val Value.
     * @param stamp Stamp.
     * @throws IgniteException If operation failed.
     */
    public void set(T val, S stamp) throws IgniteException;

    /**
     * Conditionally sets the new value and new stamp. They will be set if {@code expVal}
     * and {@code expStamp} are equal to current value and current stamp respectively.
     *
     * @param expVal Expected value.
     * @param newVal New value.
     * @param expStamp Expected stamp.
     * @param newStamp New stamp.
     * @return Result of operation execution. If {@code true} than  value and stamp will be updated.
     * @throws IgniteException If operation failed.
     */
    public boolean compareAndSet(T expVal, T newVal, S expStamp, S newStamp) throws IgniteException;

    /**
     * Gets current stamp.
     *
     * @return Current stamp.
     * @throws IgniteException If operation failed.
     */
    public S stamp() throws IgniteException;

    /**
     * Gets current value.
     *
     * @return Current value.
     * @throws IgniteException If operation failed.
     */
    public T value() throws IgniteException;

    /**
     * Gets status of atomic.
     *
     * @return {@code true} if atomic stamped was removed from cache, {@code false} otherwise.
     */
    public boolean removed();

    /**
     * Removes this atomic stamped.
     *
     * @throws IgniteException If operation failed.
     */
    @Override public void close();
}