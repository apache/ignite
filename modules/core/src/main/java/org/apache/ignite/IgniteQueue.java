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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * This interface provides a rich API for working with distributed queues based on In-Memory Data Grid.
 * <p>
 * <h1 class="header">Overview</h1>
 * Cache queue provides an access to cache elements using typical queue API. Cache queue also implements
 * {@link Collection} interface and provides all methods from collections including
 * {@link Collection#addAll(Collection)}, {@link Collection#removeAll(Collection)}, and
 * {@link Collection#retainAll(Collection)} methods for bulk operations. Note that all
 * {@link Collection} methods in the queue may throw {@link IgniteException} in case
 * of failure.
 * <p>
 * All queue operations have synchronous and asynchronous counterparts.
 * <h1 class="header">Bounded vs Unbounded</h1>
 * Queues can be {@code unbounded} or {@code bounded}. {@code Bounded} queues can
 * have maximum capacity. Queue capacity can be set at creation time and cannot be
 * changed later. Here is an example of how to create {@code bounded} {@code LIFO} queue with
 * capacity of {@code 1000} items.
 * <pre name="code" class="java">
 * IgniteQueue&lt;String&gt; queue = cache().queue("anyName", LIFO, 1000);
 * ...
 * queue.add("item");
 * </pre>
 * For {@code bounded} queues <b>blocking</b> operations, such as {@link #take()} or {@link #put(Object)}
 * are available. These operations will block until queue capacity changes to make the operation
 * possible.
 * <h1 class="header">Collocated vs Non-collocated</h1>
 * Queue items can be placed on one node or distributed throughout grid nodes
 * (governed by {@code collocated} parameter). {@code Non-collocated} mode is provided only
 * for partitioned caches. If {@code collocated} parameter is {@code true}, then all queue items
 * will be collocated on one node, otherwise items will be distributed through all grid nodes.
 * Unless explicitly specified, by default queues are {@code collocated}.
 * <p>
 * Here is an example of how create {@code unbounded} queue
 * in non-collocated mode.
 * <pre name="code" class="java">
 * IgniteQueue&lt;String&gt; queue = cache().queue("anyName", 0 &#047;*unbounded*&#047;, false &#047;*non-collocated*&#047;);
 * ...
 * queue.add("item");
 * </pre>
 * <h1 class="header">Creating Cache Queues</h1>
 * Instances of distributed cache queues can be created by calling the following method
 * on {@link Ignite} API:
 * <ul>
 *     <li>{@link Ignite#queue(String, int, org.apache.ignite.configuration.CollectionConfiguration)}</li>
 * </ul>
 * @see Ignite#queue(String, int, org.apache.ignite.configuration.CollectionConfiguration)
 */
public interface IgniteQueue<T> extends BlockingQueue<T>, Closeable {
    /**
     * Gets queue name.
     *
     * @return Queue name.
     */
    public String name();

    /** {@inheritDoc} */
    @Override public boolean add(T item) throws IgniteException;

    /** {@inheritDoc} */
    @Override public boolean offer(T item) throws IgniteException;

    /** {@inheritDoc} */
    @Override public boolean offer(T item, long timeout, TimeUnit unit) throws IgniteException;

    /** {@inheritDoc} */
    @Override public boolean addAll(Collection<? extends T> items) throws IgniteException;

    /** {@inheritDoc} */
    @Override public boolean contains(Object item) throws IgniteException;

    /** {@inheritDoc} */
    @Override public boolean containsAll(Collection<?> items) throws IgniteException;

    /** {@inheritDoc} */
    @Override public void clear() throws IgniteException;

    /** {@inheritDoc} */
    @Override public boolean remove(Object item) throws IgniteException;

    /** {@inheritDoc} */
    @Override public boolean removeAll(Collection<?> items) throws IgniteException;

    /** {@inheritDoc} */
    @Override public boolean isEmpty() throws IgniteException;

    /** {@inheritDoc} */
    @Override public Iterator<T> iterator() throws IgniteException;

    /** {@inheritDoc} */
    @Override public Object[] toArray() throws IgniteException;

    /** {@inheritDoc} */
    @Override public <T> T[] toArray(T[] a) throws IgniteException;

    /** {@inheritDoc} */
    @Override public boolean retainAll(Collection<?> items) throws IgniteException;

    /** {@inheritDoc} */
    @Override public int size() throws IgniteException;

    /** {@inheritDoc} */
    @Override public T poll() throws IgniteException;

    /** {@inheritDoc} */
    @Override public T peek() throws IgniteException;

    /** {@inheritDoc} */
    @Override public void put(T item) throws IgniteException;

    /** {@inheritDoc} */
    @Override public T take() throws IgniteException;

    /** {@inheritDoc} */
    @Override public T poll(long timeout, TimeUnit unit) throws IgniteException;

    /**
     * Removes all of the elements from this queue. Method is used in massive queues with huge numbers of elements.
     *
     * @param batchSize Batch size.
     * @throws IgniteException if operation failed.
     */
    public void clear(int batchSize) throws IgniteException;

    /**
     * Removes this queue.
     *
     * @throws IgniteException if operation failed.
     */
    @Override public void close() throws IgniteException;

    /**
     * Gets maximum number of elements of the queue.
     *
     * @return Maximum number of elements. If queue is unbounded {@code Integer.MAX_SIZE} will return.
     */
    public int capacity();

    /**
     * Returns {@code true} if this queue is bounded.
     *
     * @return {@code true} if this queue is bounded.
     */
    public boolean bounded();

    /**
     * Returns {@code true} if this queue can be kept on the one node only.
     * Returns {@code false} if this queue can be kept on the many nodes.
     *
     * @return {@code true} if this queue is in {@code collocated} mode {@code false} otherwise.
     */
    public boolean collocated();

    /**
     * Gets status of queue.
     *
     * @return {@code true} if queue was removed from cache {@code false} otherwise.
     */
    public boolean removed();
}