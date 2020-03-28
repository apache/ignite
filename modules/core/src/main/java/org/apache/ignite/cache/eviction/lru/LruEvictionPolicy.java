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
import org.apache.ignite.cache.eviction.AbstractEvictionPolicy;
import org.apache.ignite.cache.eviction.EvictableEntry;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.mxbean.IgniteMBeanAware;
import org.jsr166.ConcurrentLinkedDeque8;
import org.jsr166.ConcurrentLinkedDeque8.Node;

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
    private final ConcurrentLinkedDeque8<EvictableEntry<K, V>> queue =
        new ConcurrentLinkedDeque8<>();

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
        return queue.sizex();
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
    @Override protected boolean removeMeta(Object meta) {
        return queue.unlinkx((Node<EvictableEntry<K, V>>)meta);
    }

    /**
     * @param entry Entry to touch.
     * @return {@code True} if new node has been added to queue by this call.
     */
    @Override protected boolean touch(EvictableEntry<K, V> entry) {
        Node<EvictableEntry<K, V>> node = entry.meta();

        // Entry has not been enqueued yet.
        if (node == null) {
            while (true) {
                node = queue.offerLastx(entry);

                if (entry.putMetaIfAbsent(node) != null) {
                    // Was concurrently added, need to clear it from queue.
                    removeMeta(node);

                    // Queue has not been changed.
                    return false;
                }
                else if (node.item() != null) {
                    if (!entry.isCached()) {
                        // Was concurrently evicted, need to clear it from queue.
                        removeMeta(node);

                        return false;
                    }

                    memSize.add(entry.size());

                    return true;
                }
                // If node was unlinked by concurrent shrink() call, we must repeat the whole cycle.
                else if (!entry.removeMeta(node))
                    return false;
            }
        }
        else if (removeMeta(node)) {
            // Move node to tail.
            Node<EvictableEntry<K, V>> newNode = queue.offerLastx(entry);

            if (!entry.replaceMeta(node, newNode))
                // Was concurrently added, need to clear it from queue.
                removeMeta(newNode);
        }

        // Entry is already in queue.
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

        Node<EvictableEntry<K, V>> meta = entry.removeMeta();

        if (meta != null) {
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
