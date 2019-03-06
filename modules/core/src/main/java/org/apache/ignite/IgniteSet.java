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
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;

/**
 * Set implementation based on on In-Memory Data Grid.
 * <h1 class="header">Overview</h1>
 * Cache set implements {@link Set} interface and provides all methods from collections.
 * Note that all {@link Collection} methods in the set may throw {@link IgniteException} in case of failure
 * or if set was removed.
 * <h1 class="header">Collocated vs Non-collocated</h1>
 * Set items can be placed on one node or distributed throughout grid nodes
 * (governed by {@code collocated} parameter). {@code Non-collocated} mode is provided only
 * for partitioned caches. If {@code collocated} parameter is {@code true}, then all set items
 * will be collocated on one node, otherwise items will be distributed through all grid nodes.
 * @see Ignite#set(String, org.apache.ignite.configuration.CollectionConfiguration)
 */
public interface IgniteSet<T> extends Set<T>, Closeable {
    /** {@inheritDoc} */
    @Override boolean add(T t) throws IgniteException;

    /** {@inheritDoc} */
    @Override boolean addAll(Collection<? extends T> c) throws IgniteException;

    /** {@inheritDoc} */
    @Override void clear() throws IgniteException;

    /** {@inheritDoc} */
    @Override boolean contains(Object o) throws IgniteException;

    /** {@inheritDoc} */
    @Override boolean containsAll(Collection<?> c) throws IgniteException;

    /** {@inheritDoc} */
    @Override boolean isEmpty() throws IgniteException;

    /** {@inheritDoc} */
    @Override Iterator<T> iterator() throws IgniteException;

    /** {@inheritDoc} */
    @Override boolean remove(Object o) throws IgniteException;

    /** {@inheritDoc} */
    @Override boolean removeAll(Collection<?> c) throws IgniteException;

    /** {@inheritDoc} */
    @Override boolean retainAll(Collection<?> c) throws IgniteException;

    /** {@inheritDoc} */
    @Override int size() throws IgniteException;

    /** {@inheritDoc} */
    @Override Object[] toArray() throws IgniteException;

    /** {@inheritDoc} */
    @Override <T1> T1[] toArray(T1[] a) throws IgniteException;

    /**
     * Removes this set.
     *
     * @throws IgniteException If operation failed.
     */
    @Override public void close() throws IgniteException;

    /**
     * Gets set name.
     *
     * @return Set name.
     */
    public String name();

    /**
     * Returns {@code true} if this set can be kept on the one node only.
     * Returns {@code false} if this set can be kept on the many nodes.
     *
     * @return {@code True} if this set is in {@code collocated} mode {@code false} otherwise.
     */
    public boolean collocated();

    /**
     * Gets status of set.
     *
     * @return {@code True} if set was removed from cache {@code false} otherwise.
     */
    public boolean removed();

    /**
     * Executes given job on collocated set on the node where the set is located
     * (a.k.a. affinity co-location).
     * <p>
     * This is not supported for non-collocated sets.
     *
     * @param job Job which will be co-located with the set.
     * @throws IgniteException If job failed.
     */
    public void affinityRun(IgniteRunnable job) throws IgniteException;

    /**
     * Executes given job on collocated set on the node where the set is located
     * (a.k.a. affinity co-location).
     * <p>
     * This is not supported for non-collocated sets.
     *
     * @param job Job which will be co-located with the set.
     * @throws IgniteException If job failed.
     */
    public <R> R affinityCall(IgniteCallable<R> job) throws IgniteException;
}
