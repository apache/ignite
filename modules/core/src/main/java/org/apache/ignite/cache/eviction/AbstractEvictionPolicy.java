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

package org.apache.ignite.cache.eviction;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Common functionality implementation for eviction policies with max size/max memory and batch eviction support.
 */
public abstract class AbstractEvictionPolicy<K, V> implements EvictionPolicy<K, V>, Externalizable {
    /** */
    private static final long serialVersionUID = 4358725333474509598L;

    /** Max memory size occupied by elements in container. */
    private volatile long maxMemSize;

    /** Maximum elements in container. */
    private volatile int max;

    /** Batch size. */
    private volatile int batchSize = 1;

    /** Memory size occupied by elements in container. */
    protected final LongAdder memSize = new LongAdder();

    /**
     * Shrinks backed container to maximum allowed size.
     */
    protected void shrink() {
        long maxMem = this.maxMemSize;

        if (maxMem > 0) {
            long startMemSize = memSize.longValue();

            if (startMemSize >= maxMem) {
                for (long i = maxMem; i < startMemSize && memSize.longValue() > maxMem; ) {
                    int size = shrink0();

                    if (size == -1)
                        break;

                    i += size;
                }
            }
        }

        int max = this.max;

        if (max > 0) {
            int startSize = getCurrentSize();

            if (startSize >= max + (maxMem > 0 ? 1 : this.batchSize)) {
                for (int i = max; i < startSize && getCurrentSize() > max; i++) {
                    if (shrink0() == -1)
                        break;
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onEntryAccessed(boolean rmv, EvictableEntry<K, V> entry) {
        if (!rmv) {
            if (!entry.isCached())
                return;

            // Shrink only if queue was changed.
            if (touch(entry))
                shrink();
        }
        else {
            Object node = entry.removeMeta();

            if (node != null) {
                removeMeta(node);

                memSize.add(-entry.size());
            }
        }
    }

    /**
     * @return Size of the container with trackable entries.
     */
    protected abstract int getCurrentSize();

    /**
     *
     * @return Size of the memory which was shrinked.
     */
    protected abstract int shrink0();

    /**
     *
     * @param meta Meta-information shipped to an entry.
     * @return {@code True} if meta was successfully removed from the container.
     */
    protected abstract boolean removeMeta(Object meta);

    /**
     * @param entry Entry to touch.
     * @return {@code True} if container has been changed by this call.
     */
    protected abstract boolean touch(EvictableEntry<K, V> entry);

    /**
     * Sets maximum allowed cache size in bytes.
     * @return {@code this} for chaining.
     */
    public AbstractEvictionPolicy<K, V> setMaxMemorySize(long maxMemSize) {
        A.ensure(maxMemSize >= 0, "maxMemSize >= 0");

        this.maxMemSize = maxMemSize;

        return this;
    }

    /**
     * Gets maximum allowed cache size in bytes.
     *
     * @return maximum allowed cache size in bytes.
     */
    public long getMaxMemorySize() {
        return maxMemSize;
    }

    /**
     * Gets current queue size in bytes.
     *
     * @return current queue size in bytes.
     */
    public long getCurrentMemorySize() {
        return memSize.longValue();
    }

    /**
     * Sets maximum allowed size of cache before entry will start getting evicted.
     *
     * @param max Maximum allowed size of cache before entry will start getting evicted.
     * @return {@code this} for chaining.
     */
    public AbstractEvictionPolicy<K, V> setMaxSize(int max) {
        A.ensure(max >= 0, "max >= 0");

        this.max = max;

        return this;
    }

    /**
     * Gets maximum allowed size of cache before entry will start getting evicted.
     *
     * @return Maximum allowed size of cache before entry will start getting evicted.
     */
    public int getMaxSize() {
        return max;
    }

    /**
     * Sets batch size.
     *
     * @param batchSize Batch size.
     * @return {@code this} for chaining.
     */
    public AbstractEvictionPolicy<K, V> setBatchSize(int batchSize) {
        A.ensure(batchSize > 0, "batchSize > 0");

        this.batchSize = batchSize;

        return this;
    }

    /**
     * Gets batch size.
     *
     * @return batch size.
     */
    public int getBatchSize() {
        return batchSize;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(max);
        out.writeInt(batchSize);
        out.writeLong(maxMemSize);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        max = in.readInt();
        batchSize = in.readInt();
        maxMemSize = in.readLong();
    }
}
