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

package org.apache.ignite.cache.datastructures;

import org.apache.ignite.*;
import org.jetbrains.annotations.*;

/**
 * Facade for working with distributed cache data structures. All cache data structures are similar
 * in APIs to {@code 'java.util.concurrent'} package, but all operations on them are grid-aware.
 * For example, if you increment {@link GridCacheAtomicLong} on one node, another node will
 * know about the change. Or if you add an element to {@link GridCacheQueue} on one node,
 * you can poll it on another node.
 * <p>
 * You can get data structures facade by calling {@link org.apache.ignite.cache.GridCache#dataStructures()} method.
 */
public interface GridCacheDataStructures {
    /**
     * Will get an atomic sequence from cache and create one if it has not been created yet and {@code create} flag
     * is {@code true}.
     *
     * @param name Sequence name.
     * @param initVal Initial value for sequence. If sequence already cached, {@code initVal} will be ignored.
     * @param create Boolean flag indicating whether data structure should be created if does not exist.
     * @return Sequence for the given name.
     * @throws IgniteCheckedException If sequence could not be fetched or created.
     */
    @Nullable public GridCacheAtomicSequence atomicSequence(String name, long initVal, boolean create)
        throws IgniteCheckedException;

    /**
     * Remove sequence from cache.
     *
     * @param name Sequence name.
     * @return {@code True} if sequence has been removed, {@code false} otherwise.
     * @throws IgniteCheckedException If remove failed.
     */
    public boolean removeAtomicSequence(String name) throws IgniteCheckedException;

    /**
     * Will get a atomic long from cache and create one if it has not been created yet and {@code create} flag
     * is {@code true}.
     *
     * @param name Name of atomic long.
     * @param initVal Initial value for atomic long. If atomic long already cached, {@code initVal}
     *        will be ignored.
     * @param create Boolean flag indicating whether data structure should be created if does not exist.
     * @return Atomic long.
     * @throws IgniteCheckedException If atomic long could not be fetched or created.
     */
    @Nullable public GridCacheAtomicLong atomicLong(String name, long initVal, boolean create) throws IgniteCheckedException;

    /**
     * Remove atomic long from cache.
     *
     * @param name Name of atomic long.
     * @return {@code True} if atomic long has been removed, {@code false} otherwise.
     * @throws IgniteCheckedException If removing failed.
     */
    public boolean removeAtomicLong(String name) throws IgniteCheckedException;

    /**
     * Will get a named queue from cache and create one if it has not been created yet and {@code create} flag
     * is {@code true}.
     * If queue is present in cache already, queue properties will not be changed. Use
     * collocation for {@link org.apache.ignite.cache.GridCacheMode#PARTITIONED} caches if you have lots of relatively
     * small queues as it will make fetching, querying, and iteration a lot faster. If you have
     * few very large queues, then you should consider turning off collocation as they simply
     * may not fit in a single node's memory. However note that in this case
     * to get a single element off the queue all nodes may have to be queried.
     *
     * @param name Name of queue.
     * @param cap Capacity of queue, {@code 0} for unbounded queue.
     * @param collocated If {@code true} then all items within the same queue will be collocated on the same node.
     *      Otherwise elements of the same queue maybe be cached on different nodes. If you have lots of relatively
     *      small queues, then you should use collocation. If you have few large queues, then you should turn off
     *      collocation. This parameter works only for {@link org.apache.ignite.cache.GridCacheMode#PARTITIONED} cache.
     * @param create Boolean flag indicating whether data structure should be created if does not exist.
     * @return Queue with given properties.
     * @throws IgniteCheckedException If remove failed.
     */
    @Nullable public <T> GridCacheQueue<T> queue(String name, int cap, boolean collocated,
        boolean create) throws IgniteCheckedException;

    /**
     * Remove queue from cache. Internally one transaction will be created for all elements
     * in the queue. If you anticipate that queue may be large, then it's better to use
     * {@link #removeQueue(String, int)} which allows to specify batch size. In that case
     * transaction will be split into multiple transactions which will have upto {@code batchSize}
     * elements in it.
     *
     * @param name Name queue.
     * @return {@code True} if queue has been removed and false if it's not cached.
     * @throws IgniteCheckedException If remove failed.
     */
    public boolean removeQueue(String name) throws IgniteCheckedException;

    /**
     * Remove queue from cache. Internally multiple transactions will be created
     * with no more than {@code batchSize} elements in them. For larger queues, this
     * method is preferrable over {@link #removeQueue(String)} which will create only
     * one transaction for the whole operation.
     *
     * @param name Name queue.
     * @param batchSize Batch size.
     * @return {@code True} if queue has been removed and false if it's not cached.
     * @throws IgniteCheckedException If remove failed.
     */
    public boolean removeQueue(String name, int batchSize) throws IgniteCheckedException;

    /**
     * Will get a named set from cache and create one if it has not been created yet and {@code create} flag
     * is {@code true}.
     *
     * @param name Set name.
     * @param collocated If {@code true} then all items within the same set will be collocated on the same node.
     *      Otherwise elements of the same set maybe be cached on different nodes. This parameter works only
     *      for {@link org.apache.ignite.cache.GridCacheMode#PARTITIONED} cache.
     * @param create Flag indicating whether set should be created if does not exist.
     * @return Set with given properties.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public <T> GridCacheSet<T> set(String name, boolean collocated, boolean create) throws IgniteCheckedException;

    /**
     * Removes set from cache.
     *
     * @param name Set name.
     * @return {@code True} if set has been removed and false if it's not cached.
     * @throws IgniteCheckedException If failed.
     */
    public boolean removeSet(String name) throws IgniteCheckedException;

    /**
     * Will get a atomic reference from cache and create one if it has not been created yet and {@code create} flag
     * is {@code true}.
     *
     * @param name Atomic reference name.
     * @param initVal Initial value for atomic reference. If atomic reference already cached,
     *      {@code initVal} will be ignored.
     * @param create Boolean flag indicating whether data structure should be created if does not exist.
     * @return Atomic reference for the given name.
     * @throws IgniteCheckedException If atomic reference could not be fetched or created.
     */
    @Nullable public <T> GridCacheAtomicReference<T> atomicReference(String name, @Nullable T initVal, boolean create)
        throws IgniteCheckedException;

    /**
     * Remove atomic reference from cache.
     *
     * @param name Atomic reference name.
     * @return {@code True} if atomic reference has been removed, {@code false} otherwise.
     * @throws IgniteCheckedException If remove failed.
     */
    public boolean removeAtomicReference(String name) throws IgniteCheckedException;

    /**
     * Will get a atomic stamped from cache and create one if it has not been created yet and {@code create} flag
     * is {@code true}.
     *
     * @param name Atomic stamped name.
     * @param initVal Initial value for atomic stamped. If atomic stamped already cached,
     *      {@code initVal} will be ignored.
     * @param initStamp Initial stamp for atomic stamped. If atomic stamped already cached,
     *      {@code initStamp} will be ignored.
     * @param create Boolean flag indicating whether data structure should be created if does not exist.
     * @return Atomic stamped for the given name.
     * @throws IgniteCheckedException If atomic stamped could not be fetched or created.
     */
    @Nullable public <T, S> GridCacheAtomicStamped<T, S> atomicStamped(String name, @Nullable T initVal,
        @Nullable S initStamp, boolean create) throws IgniteCheckedException;

    /**
     * Remove atomic stamped from cache.
     *
     * @param name Atomic stamped name.
     * @return {@code True} if atomic stamped has been removed, {@code false} otherwise.
     * @throws IgniteCheckedException If remove failed.
     */
    public boolean removeAtomicStamped(String name) throws IgniteCheckedException;

    /**
     * Gets or creates count down latch. If count down latch is not found in cache and {@code create} flag
     * is {@code true}, it is created using provided name and count parameter.
     *
     * @param name Name of the latch.
     * @param cnt Count for new latch creation.
     * @param autoDel {@code True} to automatically delete latch from cache
     *      when its count reaches zero.
     * @param create Boolean flag indicating whether data structure should be created if does not exist.
     * @return Count down latch for the given name.
     * @throws IgniteCheckedException If operation failed.
     */
    @Nullable public GridCacheCountDownLatch countDownLatch(String name, int cnt, boolean autoDel, boolean create)
        throws IgniteCheckedException;

    /**
     * Removes count down latch from cache.
     *
     * @param name Name of the latch.
     * @return Count down latch for the given name.
     * @throws IgniteCheckedException If operation failed.
     */
    public boolean removeCountDownLatch(String name) throws IgniteCheckedException;
}
