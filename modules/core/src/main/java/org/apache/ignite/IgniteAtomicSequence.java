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

/**
 * This interface provides a rich API for working with distributed atomic sequence.
 * <p>
 * <h1 class="header">Functionality</h1>
 * Distributed atomic sequence includes the following main functionality:
 * <ul>
 * <li>
 * Method {@link #get()} gets current value from atomic sequence.
 * </li>
 * <li>
 * Various {@code get..(..)} methods get current value from atomic sequence
 * and increase atomic sequences value.
 * </li>
 * <li>
 * Various {@code add..(..)} {@code increment(..)} methods increase atomic sequences value
 * and return increased value.
 * </li>
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