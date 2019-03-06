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
 * This interface provides a rich API for working with distributed atomic reference.
 * <p>
 * <h1 class="header">Functionality</h1>
 * Distributed atomic reference includes the following main functionality:
 * <ul>
 * <li>
 * Method {@link #get()} gets current value of an atomic reference.
 * </li>
 * <li>
 * Method {@link #set(Object)} unconditionally sets the value in the an atomic reference.
 * </li>
 * <li>
 * Methods {@code compareAndSet(...)} conditionally set the value in the an atomic reference.
 * </li>
 * <li>
 * Method {@link #name()} gets name of atomic reference.
 * </li>
 * </ul>
 * <h1 class="header">Creating Distributed Atomic Reference</h1>
 * Instance of distributed atomic reference can be created by calling the following method:
 * <ul>
 *     <li>{@link Ignite#atomicReference(String, Object, boolean)}</li>
 * </ul>
 * @see Ignite#atomicReference(String, Object, boolean)
 */
public interface IgniteAtomicReference<T> extends Closeable {
    /**
     * Name of atomic reference.
     *
     * @return Name of an atomic reference.
     */
    public String name();

    /**
     * Gets current value of an atomic reference.
     *
     * @return current value of an atomic reference.
     * @throws IgniteException If operation failed.
     */
    public T get() throws IgniteException;

    /**
     * Unconditionally sets the value.
     *
     * @param val Value.
     * @throws IgniteException If operation failed.
     */
    public void set(T val) throws IgniteException;

    /**
     * Conditionally sets the new value. That will be set if {@code expVal} is equal
     * to current value respectively.
     *
     * @param expVal Expected value.
     * @param newVal New value.
     * @return Result of operation execution. If {@code true} than value have been updated.
     * @throws IgniteException If operation failed.
     */
    public boolean compareAndSet(T expVal, T newVal) throws IgniteException;

    /**
     * Gets status of atomic.
     *
     * @return {@code true} if an atomic reference was removed from cache, {@code false} otherwise.
     */
    public boolean removed();

    /**
     * Removes this atomic reference.
     *
     * @throws IgniteException If operation failed.
     */
    @Override public void close();
}