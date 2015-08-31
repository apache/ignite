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
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

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
}