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

package org.apache.ignite.cache.eviction.sorted;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.cache.eviction.AbstractEvictionPolicy;
import org.apache.ignite.cache.eviction.EvictableEntry;
import org.apache.ignite.internal.util.GridConcurrentSkipListSet;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.mxbean.IgniteMBeanAware;
import org.jetbrains.annotations.Nullable;

import static java.lang.Math.abs;
import static org.apache.ignite.configuration.CacheConfiguration.DFLT_CACHE_SIZE;

/**
 * Cache eviction policy which will select the minimum cache entry for eviction.
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
 * <p>
 * Entries comparison based on {@link Comparator} instance if provided.
 * Default {@code Comparator} behaviour is use cache entries keys for comparison that imposes a requirement for keys
 * to implement {@link Comparable} interface.
 * <p>
 * User defined comparator should implement {@link Serializable} interface.
 */
public class SortedEvictionPolicy<K, V> extends AbstractEvictionPolicy<K, V> implements IgniteMBeanAware {
    /** */
    private static final long serialVersionUID = 0L;

    /** Comparator. */
    private Comparator<Holder<K, V>> comp;

    /** Order. */
    private final AtomicLong orderCnt = new AtomicLong();

    /** Backed sorted queue. */
    private final GridConcurrentSkipListSetEx<K, V> set;

    /**
     * Constructs sorted eviction policy with all defaults.
     */
    public SortedEvictionPolicy() {
        this(DFLT_CACHE_SIZE, null);
    }

    /**
     * Constructs sorted eviction policy with maximum size.
     *
     * @param max Maximum allowed size of cache before entry will start getting evicted.
     */
    public SortedEvictionPolicy(int max) {
        this(max, null);
    }

    /**
     * Constructs sorted eviction policy with given maximum size and given entry comparator.
     *
     * @param max Maximum allowed size of cache before entry will start getting evicted.
     * @param comp Entries comparator.
     */
    public SortedEvictionPolicy(int max, @Nullable Comparator<EvictableEntry<K, V>> comp) {
        this(max, 1, comp);
    }

    /**
     * Constructs sorted eviction policy with given maximum size, eviction batch size and entries comparator.
     *
     * @param max Maximum allowed size of cache before entry will start getting evicted.
     * @param batchSize Batch size.
     * @param comp Entries comparator.
     */
    public SortedEvictionPolicy(int max, int batchSize, @Nullable Comparator<EvictableEntry<K, V>> comp) {
        setMaxSize(max);
        setBatchSize(batchSize);

        this.comp = comp == null ? new DefaultHolderComparator<K, V>() : new HolderComparator<>(comp);
        this.set = new GridConcurrentSkipListSetEx<>(this.comp);
    }

    /**
     * Constructs sorted eviction policy with given maximum size and given entry comparator.
     *
     * @param comp Entries comparator.
     */
    public SortedEvictionPolicy(@Nullable Comparator<EvictableEntry<K, V>> comp) {
        this.comp = comp == null ? new DefaultHolderComparator<K, V>() : new HolderComparator<>(comp);
        this.set = new GridConcurrentSkipListSetEx<>(this.comp);
    }

    /** {@inheritDoc} */
    @Override public SortedEvictionPolicy<K, V> setMaxMemorySize(long maxMemSize) {
        super.setMaxMemorySize(maxMemSize);

        return this;
    }

    /** {@inheritDoc} */
    @Override public SortedEvictionPolicy<K, V> setMaxSize(int max) {
        super.setMaxSize(max);

        return this;
    }

    /** {@inheritDoc} */
    @Override public SortedEvictionPolicy<K, V> setBatchSize(int batchSize) {
        super.setBatchSize(batchSize);

        return this;
    }

    /**
     * Gets read-only view of backed queue in proper order.
     *
     * @return Read-only view of backed queue.
     */
    public Collection<EvictableEntry<K, V>> queue() {
        Set<EvictableEntry<K, V>> cp = new LinkedHashSet<>();

        for (Holder<K, V> holder : set)
            cp.add(holder.entry);

        return Collections.unmodifiableCollection(cp);
    }

    /**
     * @param entry Entry to touch.
     * @return {@code True} if backed queue has been changed by this call.
     */
    @Override protected boolean touch(EvictableEntry<K, V> entry) {
        Holder<K, V> holder = entry.meta();

        // Entry has not been added yet to backed queue.
        if (holder == null) {
            while (true) {
                holder = new Holder<>(entry, orderCnt.incrementAndGet());

                if (entry.putMetaIfAbsent(holder) != null)
                    return false; // Set has not been changed.

                set.add(holder);

                if (holder.order > 0) {
                    if (!entry.isCached()) {
                        // Was concurrently evicted, need to remove it from queue.
                        removeMeta(holder);

                        return false;
                    }

                    memSize.add(entry.size());

                    return true;
                }
                // If holder was removed by concurrent shrink() call, we must repeat the whole cycle.
                else if (!entry.removeMeta(holder))
                    return false;
            }
        }

        // Entry is already in queue.
        return false;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentSize() {
        return set.sizex();
    }

    /**
     * Tries to remove one item from queue.
     *
     * @return number of bytes that was free. {@code -1} if queue is empty.
     */
    @Override protected int shrink0() {
        Holder<K, V> h = set.pollFirst();

        if (h == null)
            return -1;

        int size = 0;

        EvictableEntry<K, V> entry = h.entry;

        assert entry != null;

        if (h.order > 0 && entry.removeMeta(h)) {
            size = entry.size();

            memSize.add(-size);

            if (!entry.evict())
                touch(entry);
        }

        return size;
    }

    /** {@inheritDoc} */
    @Override public Object getMBean() {
        return new SortedEvictionPolicyMBeanImpl();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(comp);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        comp = (Comparator<Holder<K, V>>)in.readObject();
    }

    /**
     * Removes holder from backed queue and marks holder as removed.
     *
     * @param meta Holder.
     */
    @Override protected boolean removeMeta(Object meta) {
        Holder<K, V> holder = (Holder<K, V>)meta;

        long order0 = holder.order;

        if (order0 > 0)
            holder.order = -order0;

        return set.remove(holder);
    }

    /**
     * Evictable entry holder.
     */
    private static class Holder<K, V> {
        /** Entry. */
        private final EvictableEntry<K, V> entry;

        /** Order needs for distinguishing keys that are equal. */
        private volatile long order;

        /**
         * Constructs holder for given key.
         *
         * @param entry Entry.
         * @param order Order.
         */
        public Holder(EvictableEntry<K, V> entry, long order) {
            assert order > 0;

            this.entry = entry;
            this.order = order;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return entry.hashCode();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (this == obj)
                return true;

            if (obj == null || this.getClass() != obj.getClass())
                return false;

            Holder<K, V> h = (Holder<K, V>) obj;

            return Objects.equals(entry, h.entry) && abs(order) == abs(h.order);
        }
    }

    /**
     * Evictable entries holder comparator. Wrapper for client's comparator.
     */
    private static class HolderComparator<K, V> implements Comparator<Holder<K, V>>, Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Keys comparator. */
        private final Comparator<EvictableEntry<K, V>> comp;

        /**
         * @param comp Comparator.
         */
        public HolderComparator(Comparator<EvictableEntry<K, V>> comp) {
            A.notNull(comp, "comp");

            this.comp = comp;
        }

        /** {@inheritDoc} */
        @Override public int compare(Holder<K, V> h1, Holder<K, V> h2) {
            if (h1 == h2)
                return 0;

            EvictableEntry<K, V> e1 = h1.entry;
            EvictableEntry<K, V> e2 = h2.entry;

            int cmp = comp.compare(e1, e2);

            return cmp == 0 ? Long.compare(abs(h1.order), abs(h2.order)) : cmp;
        }
    }

    /**
     * Default comparator. Uses if comparator isn't provided by client.
     * Compares only entry keys that should implements {@link Comparable} interface.
     */
    private static class DefaultHolderComparator<K, V> implements Comparator<Holder<K, V>>, Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public int compare(Holder<K, V> h1, Holder<K, V> h2) {
            if (h1 == h2)
                return 0;

            EvictableEntry<K, V> e1 = h1.entry;
            EvictableEntry<K, V> e2 = h2.entry;

            int cmp = ((Comparable<K>)e1.getKey()).compareTo(e2.getKey());

            return cmp == 0 ? Long.compare(abs(h1.order), abs(h2.order)) : cmp;
        }
    }

    /**
     * Provides additional method {@code #sizex()}. NOTE: Only the following methods supports this addition:
     * <ul>
     *     <li>{@code #add()}</li>
     *     <li>{@code #remove()}</li>
     *     <li>{@code #pollFirst()}</li>
     *     <li>{@code #clone()}</li>
     * <ul/>
     */
    private static class GridConcurrentSkipListSetEx<K, V> extends GridConcurrentSkipListSet<Holder<K, V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Size. */
        private final LongAdder size = new LongAdder();

        /**
         * @param comp Comparator.
         */
        public GridConcurrentSkipListSetEx(Comparator<? super Holder<K, V>> comp) {
            super(comp);
        }

        /**
         * @return Size based on performed operations.
         */
        public int sizex() {
            return size.intValue();
        }

        /** {@inheritDoc} */
        @Override public boolean add(Holder<K, V> e) {
            boolean res = super.add(e);

            assert res;

            size.increment();

            return res;
        }

        /** {@inheritDoc} */
        @Override public boolean remove(Object o) {
            boolean res = super.remove(o);

            if (res)
                size.decrement();

            return res;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Holder<K, V> pollFirst() {
            Holder<K, V> e = super.pollFirst();

            if (e != null)
                size.decrement();

            return e;
        }
    }

    /**
     * MBean implementation for SortedEvictionPolicy.
     */
    private class SortedEvictionPolicyMBeanImpl implements SortedEvictionPolicyMBean {
        /** {@inheritDoc} */
        @Override public long getCurrentMemorySize() {
            return SortedEvictionPolicy.this.getCurrentMemorySize();
        }

        /** {@inheritDoc} */
        @Override public int getCurrentSize() {
            return SortedEvictionPolicy.this.getCurrentSize();
        }

        /** {@inheritDoc} */
        @Override public int getMaxSize() {
            return SortedEvictionPolicy.this.getMaxSize();
        }

        /** {@inheritDoc} */
        @Override public void setMaxSize(int max) {
            SortedEvictionPolicy.this.setMaxSize(max);
        }

        /** {@inheritDoc} */
        @Override public int getBatchSize() {
            return SortedEvictionPolicy.this.getBatchSize();
        }

        /** {@inheritDoc} */
        @Override public void setBatchSize(int batchSize) {
            SortedEvictionPolicy.this.setBatchSize(batchSize);
        }

        /** {@inheritDoc} */
        @Override public long getMaxMemorySize() {
            return SortedEvictionPolicy.this.getMaxMemorySize();
        }

        /** {@inheritDoc} */
        @Override public void setMaxMemorySize(long maxMemSize) {
            SortedEvictionPolicy.this.setMaxMemorySize(maxMemSize);
        }
    }
}
