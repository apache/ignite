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

package org.apache.ignite.cache.eviction.lru;

import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.ignite.cache.eviction.AbstractEvictionPolicy;
import org.apache.ignite.cache.eviction.EvictableEntry;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.mxbean.IgniteMBeanAware;
import org.apache.ignite.util.deque.LongSizeCountingDeque;

/**
 * Eviction policy based on {@code Least Recently Used (LRU)} algorithm and supports batch eviction.
 * <p>
 * The eviction starts in the following cases:
 * <ul>
 *     <li>The cache size becomes {@code batchSize} elements greater than the maximum size.</li>
 *     <li>
 *         The size of cache entries in bytes becomes greater than the maximum memory size.
 *         The size of cache entry calculates as sum of key size and value size.
 *     </li>
 * </ul>
 * <b>Note:</b>Batch eviction is enabled only if maximum memory limit isn't set ({@code maxMemSize == 0}).
 * {@code batchSize} elements will be evicted in this case. The default {@code batchSize} value is {@code 1}.

 * This implementation is very efficient since it is lock-free and does not create any additional table-like
 * data structures. The {@code LRU} ordering information is maintained by attaching ordering metadata to cache entries.
 */
public class LruEvictionPolicy<K, V> extends AbstractEvictionPolicy<K, V> implements IgniteMBeanAware {
    /** */
    private static final long serialVersionUID = 0L;

    /** Queue. */
    private final Deque<EvictableEntry<K, V>> queue = new LongSizeCountingDeque<>(new ConcurrentLinkedDeque<>());

    /**
     * Constructs LRU eviction policy with all defaults.
     */
    public LruEvictionPolicy() {
        // No-op.
    }

    /**
     * Constructs LRU eviction policy with maximum size.
     *
     * @param max Maximum allowed size of cache before entry will start getting evicted.
     */
    public LruEvictionPolicy(int max) {
        setMaxSize(max);
    }

    /** {@inheritDoc} */
    @Override public int getCurrentSize() {
        return queue.size();
    }

    /** {@inheritDoc} */
    @Override public LruEvictionPolicy<K, V> setMaxMemorySize(long maxMemSize) {
        super.setMaxMemorySize(maxMemSize);

        return this;
    }

    /** {@inheritDoc} */
    @Override public LruEvictionPolicy<K, V> setMaxSize(int max) {
        super.setMaxSize(max);

        return this;
    }

    /** {@inheritDoc} */
    @Override public LruEvictionPolicy<K, V> setBatchSize(int batchSize) {
        super.setBatchSize(batchSize);

        return this;
    }

    /**
     * Gets read-only view on internal {@code FIFO} queue in proper order.
     *
     * @return Read-only view ono internal {@code 'FIFO'} queue.
     */
    public Collection<EvictableEntry<K, V>> queue() {
        return Collections.unmodifiableCollection(queue);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected boolean removeMeta(Object meta) {
        return queue.remove(meta);
    }

    /**
     * @param entry Entry to touch.
     * @return {@code True} if new node has been added to queue by this call.
     */
    @Override protected boolean touch(EvictableEntry<K, V> entry) {
        EvictableEntry<K, V> meta = entry.meta();

        // Entry has not been enqueued yet.
        if (meta == null) {
            // 1 - put entry to the queue, 2 - mark entry as queued through CAS on entry meta.
            // Strictly speaking, this order does not provides correct concurrency,
            // but inconsistency effect is negligible, unlike the opposite order.
            // See shrink0 and super.onEntryAccessed().
            queue.offerLast(entry);

            if (!entry.isCached() || entry.putMetaIfAbsent(entry) != null) {
                queue.remove(entry);

                return false;
            }

            memSize.add(entry.size());

            return true;
        }

        if (removeMeta(meta)) {
            // Move entry to tail.
            queue.offerLast(entry);
        }

        return false;
    }

    /**
     * Tries to remove one item from queue.
     *
     * @return number of bytes that was free. {@code -1} if queue is empty.
     */
    @Override protected int shrink0() {
        EvictableEntry<K, V> entry = queue.poll();

        if (entry == null)
            return -1;

        int size = 0;

        EvictableEntry<K, V> self = entry.removeMeta();

        if (self != null) {
            size = entry.size();

            memSize.add(-size);

            if (!entry.evict())
                touch(entry);
        }

        return size;
    }

    /** {@inheritDoc} */
    @Override public Object getMBean() {
        return new LruEvictionPolicyMBeanImpl();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(LruEvictionPolicy.class, this, "size", getCurrentSize());
    }

    /**
     * MBean implementation for LruEvictionPolicy.
     */
    private class LruEvictionPolicyMBeanImpl implements LruEvictionPolicyMBean {
        /** {@inheritDoc} */
        @Override public long getCurrentMemorySize() {
            return LruEvictionPolicy.this.getCurrentMemorySize();
        }

        /** {@inheritDoc} */
        @Override public int getCurrentSize() {
            return LruEvictionPolicy.this.getCurrentSize();
        }

        /** {@inheritDoc} */
        @Override public int getMaxSize() {
            return LruEvictionPolicy.this.getMaxSize();
        }

        /** {@inheritDoc} */
        @Override public void setMaxSize(int max) {
            LruEvictionPolicy.this.setMaxSize(max);
        }

        /** {@inheritDoc} */
        @Override public int getBatchSize() {
            return LruEvictionPolicy.this.getBatchSize();
        }

        /** {@inheritDoc} */
        @Override public void setBatchSize(int batchSize) {
            LruEvictionPolicy.this.setBatchSize(batchSize);
        }

        /** {@inheritDoc} */
        @Override public long getMaxMemorySize() {
            return LruEvictionPolicy.this.getMaxMemorySize();
        }

        /** {@inheritDoc} */
        @Override public void setMaxMemorySize(long maxMemSize) {
            LruEvictionPolicy.this.setMaxMemorySize(maxMemSize);
        }
    }
}