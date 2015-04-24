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

import org.apache.ignite.cache.eviction.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import org.jetbrains.annotations.*;
import org.jsr166.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static java.lang.Math.*;
import static org.apache.ignite.configuration.CacheConfiguration.*;

/**
 * Cache eviction policy which will select the minimum cache entry for eviction.
 * <p>
 * The eviction starts when the cache size becomes {@code batchSize} elements greater than the maximum size.
 * {@code batchSize} elements will be evicted in this case. The default {@code batchSize} value is {@code 1}.
 * <p>
 * Entries comparison based on {@link Comparator} instance if provided.
 * Default {@code Comparator} behaviour is use cache entries keys for comparison that imposes a requirement for keys
 * to implement {@link Comparable} interface.
 * <p>
 * User defined comparator should implement {@link Serializable} interface.
 */
public class SortedEvictionPolicy<K, V> implements EvictionPolicy<K, V>, SortedEvictionPolicyMBean, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Maximum size. */
    private volatile int max;

    /** Batch size. */
    private volatile int batchSize = 1;

    /** Comparator. */
    private Comparator<Holder<K, V>> comp;

    /** Order. */
    private final AtomicLong orderCnt = new AtomicLong();

    /** Backed sorted set. */
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
        A.ensure(max > 0, "max > 0");
        A.ensure(batchSize > 0, "batchSize > 0");

        this.max = max;
        this.batchSize = batchSize;
        this.comp = comp == null ? new DefaultHolderComparator<K, V>() : new HolderComparator<>(comp);
        this.set = new GridConcurrentSkipListSetEx<>(this.comp);
    }

    /**
     * Gets maximum allowed size of cache before entry will start getting evicted.
     *
     * @return Maximum allowed size of cache before entry will start getting evicted.
     */
    @Override public int getMaxSize() {
        return max;
    }

    /**
     * Sets maximum allowed size of cache before entry will start getting evicted.
     *
     * @param max Maximum allowed size of cache before entry will start getting evicted.
     */
    @Override public void setMaxSize(int max) {
        A.ensure(max > 0, "max > 0");

        this.max = max;
    }

    /** {@inheritDoc} */
    @Override public int getBatchSize() {
        return batchSize;
    }

    /** {@inheritDoc} */
    @Override public void setBatchSize(int batchSize) {
        A.ensure(batchSize > 0, "batchSize > 0");

        this.batchSize = batchSize;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentSize() {
        return set.sizex();
    }

    /**
     * Gets read-only view of backed set in proper order.
     *
     * @return Read-only view of backed set.
     */
    public Collection<EvictableEntry<K, V>> set() {
        Set<EvictableEntry<K, V>> cp = new LinkedHashSet<>();

        for (Holder<K, V> holder : set)
            cp.add(holder.entry);

        return Collections.unmodifiableCollection(cp);
    }

    /** {@inheritDoc} */
    @Override public void onEntryAccessed(boolean rmv, EvictableEntry<K, V> entry) {
        if (!rmv) {
            if (!entry.isCached())
                return;

            if (touch(entry))
                shrink();
        }
        else {
            Holder<K, V> holder = entry.removeMeta();

            if (holder != null)
                removeHolder(holder);
        }
    }

    /**
     * @param entry Entry to touch.
     * @return {@code True} if backed set has been changed by this call.
     */
    private boolean touch(EvictableEntry<K, V> entry) {
        Holder<K, V> holder = entry.meta();

        // Entry has not been add yet to backed set..
        if (holder == null) {
            while (true) {
                holder = new Holder<>(entry, orderCnt.incrementAndGet());

                set.add(holder);

                if (entry.putMetaIfAbsent(holder) != null) {
                    // Was concurrently added, need to remove it from set.
                    removeHolder(holder);

                    // Set has not been changed.
                    return false;
                }
                else if (holder.order > 0) {
                    if (!entry.isCached()) {
                        // Was concurrently evicted, need to remove it from set.
                        removeHolder(holder);

                        return false;
                    }

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

    /**
     * Shrinks backed set to maximum allowed size.
     */
    private void shrink() {
        int max = this.max;

        int batchSize = this.batchSize;

        int startSize = set.sizex();

        if (startSize >= max + batchSize) {
            for (int i = max; i < startSize && set.sizex() > max; i++) {
                Holder<K, V> h = set.pollFirst();

                if (h == null)
                    break;

                EvictableEntry<K, V> entry = h.entry;

                if (h.order > 0 && entry.removeMeta(h) && !entry.evict())
                    touch(entry);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(max);
        out.writeInt(batchSize);
        out.writeObject(comp);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        max = in.readInt();
        batchSize = in.readInt();
        comp = (Comparator<Holder<K, V>>)in.readObject();
    }

    /**
     * Removes holder from backed set and marks holder as removed.
     *
     * @param holder Holder.
     */
    private void removeHolder(Holder<K, V> holder) {
        long order0 = holder.order;

        if (order0 > 0)
            holder.order = -order0;

        set.remove(holder);
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
        @SuppressWarnings("unchecked")
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
        @SuppressWarnings("unchecked")
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
        private volatile LongAdder8 size = new LongAdder8();

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
}
