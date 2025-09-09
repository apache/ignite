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
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * Distributed Set.
 *
 * <h1 class="header">Overview</h1>
 * Cache set implements {@link Set} interface and provides all methods from collections.
 *
 * <h1 class="header">Colocated vs Non-colocated</h1>
 * Set items can be placed on one node or distributed across grid nodes
 * (governed by {@link ClientCollectionConfiguration#setColocated(boolean)} parameter).
 * {@code Non-colocated} mode is provided only for partitioned caches.
 * If {@code colocated} parameter is {@code true}, then all set items
 * will be colocated on one node, otherwise items will be distributed across all grid nodes.
 *
 * @see IgniteClient#set(String, org.apache.ignite.client.ClientCollectionConfiguration)
 */
public interface ClientIgniteSet<T> extends Set<T>, Closeable {
    /** {@inheritDoc} */
    @Override boolean add(T o);

    /** {@inheritDoc} */
    @Override boolean addAll(Collection<? extends T> c);

    /** {@inheritDoc} */
    @Override void clear();

    /** {@inheritDoc} */
    @Override boolean contains(Object o);

    /** {@inheritDoc} */
    @Override boolean containsAll(Collection<?> c);

    /** {@inheritDoc} */
    @Override boolean isEmpty();

    /**
     * Returns an iterator over the elements in this collection.
     * <p>
     * There are no guarantees concerning the order in which the elements are returned.
     * <p>
     * Returned iterator is {@link AutoCloseable}: it may hold server-side resources and must be closed.
     * It will close itself when the last page of data (see {@link #pageSize()}) is fetched from the server.
     * When {@link Iterator#hasNext()} returns {@code false}, it is guaranteed that the iterator is closed.
     * In other cases (incomplete iteration) the user must close the iterator.
     *
     * @return an Iterator over the elements in this collection.
     */
    @Override ClientAutoCloseableIterator<T> iterator();

    /** {@inheritDoc} */
    @Override boolean remove(Object o);

    /** {@inheritDoc} */
    @Override boolean removeAll(Collection<?> c);

    /** {@inheritDoc} */
    @Override boolean retainAll(Collection<?> c);

    /** {@inheritDoc} */
    @Override int size();

    /** {@inheritDoc} */
    @Override Object[] toArray();

    /** {@inheritDoc} */
    @Override <T1> T1[] toArray(T1[] a);

    /**
     * Removes this set.
     */
    @Override public void close();

    /**
     * Gets set name.
     *
     * @return Set name.
     */
    public String name();

    /**
     * Gets a value indicating whether all items of this set are stored on a single node.
     *
     * @return {@code True} if all items of this set are stored on a single node, {@code false} otherwise.
     */
    public boolean colocated();

    /**
     * Gets a value indicating whether this set has been removed ({@link #close()} was called).
     *
     * @return {@code True} if set was removed from cache, {@code false} otherwise.
     */
    public boolean removed();

    /**
     * Sets a value indicating whether user objects should be kept in binary form on the server, or deserialized.
     * <p>
     * Default is {@code true}: does not require classes on server, interoperable with other thin clients, performs better.
     * Suitable for most use cases.
     * <p>
     * Set to {@code false} if there is a requirement to use deserialized objects in "thick" API ({@link org.apache.ignite.IgniteSet})
     * together with thin client API, like in this scenario:
     *
     *  <pre> {@code
     *  ClientIgniteSet<UserObj> clientSet = client.set("my-set", new ClientCollectionConfiguration());
     *  clientSet.serverKeepBinary(false);
     *
     *  IgniteSet<UserObj> serverSet = server.set(clientSet.name(), null);
     *
     *  clientSet.add(new UserObj(1, "client"));
     *  assert serverSet.contains(new UserObj(1, "client"));
     * }</pre>
     *
     * @param keepBinary Whether to keep objects in binary form on the server.
     * @return This set instance (for chaining).
     */
    public ClientIgniteSet<T> serverKeepBinary(boolean keepBinary);

    /**
     * Gets a value indicating whether user objects should be kept in binary form on the server, or deserialized.
     * <p>
     * Default is {@code true}: does not require classes on server, interoperable with other thin clients, performs better.
     * Suitable for most use cases.
     * <p>
     * Set to {@code false} if there is a requirement to use deserialized objects in "thick" API ({@link org.apache.ignite.IgniteSet})
     * together with thin client API, like in this scenario:
     *
     *  <pre> {@code
     *  ClientIgniteSet<UserObj> clientSet = client.set("my-set", new ClientCollectionConfiguration());
     *  clientSet.serverKeepBinary(false);
     *
     *  IgniteSet<UserObj> serverSet = server.set(clientSet.name(), null);
     *
     *  clientSet.add(new UserObj(1, "client"));
     *  assert serverSet.contains(new UserObj(1, "client"));
     * }</pre>
     *
     * @return {@code true} when user objects will be kept in binary form on the server, {@code false} otherwise.
     */
    public boolean serverKeepBinary();

    /**
     * Sets the page size to be used for batched network data retrieval in {@link #iterator()} and {@link #toArray()}.
     *
     * @param pageSize Page size.
     * @return This set instance (for chaining).
     */
    public ClientIgniteSet<T> pageSize(int pageSize);

    /**
     * Gets the page size to be used for batched network data retrieval in {@link #iterator()} and {@link #toArray()}.
     *
     * @return Page size.
     */
    public int pageSize();
}
