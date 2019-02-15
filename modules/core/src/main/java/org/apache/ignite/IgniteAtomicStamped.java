/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
 * Method {@link #get()} gets both value and stamp of atomic.
 * </li>
 * <li>
 * Method {@link #value()} gets current value of atomic.
 * </li>
 * <li>
 * Method {@link #stamp()} gets current stamp of atomic.
 * </li>
 * <li>
 * Method {@link #set(Object, Object)} unconditionally sets the value
 * and the stamp in the atomic.
 * </li>
 * <li>
 * Methods {@code compareAndSet(...)} conditionally set the value
 * and the stamp in the atomic.
 * </li>
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