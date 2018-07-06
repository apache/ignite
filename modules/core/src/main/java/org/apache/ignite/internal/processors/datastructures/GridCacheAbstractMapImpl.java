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

package org.apache.ignite.internal.processors.datastructures;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

public abstract class GridCacheAbstractMapImpl<V> {
    /** Value returned by closure updating multimap header indicating that multimap was removed. */
    protected static final long MULTIMAP_REMOVED_IDX = Long.MIN_VALUE;

    /** Cache context. */
    protected final GridCacheContext<?, ?> cctx;

    /** Map header key. */
    protected final GridCacheMapHeaderKey hdrKey;

    /** Map unique ID. */
    protected final GridCacheMapHeader hdr;

    /** Cache. */
    protected final IgniteInternalCache<MapItemKey, V> cache;

    /** Access to affinityRun() and affinityCall() functions. */
    protected final IgniteCompute compute;

    /** Map name. */
    protected final String name;

    /**
     * @param cctx Cache context.
     * @param name Map name.
     * @param hdr Map hdr.
     */
    public GridCacheAbstractMapImpl(GridCacheContext cctx, String name, GridCacheMapHeader hdr) {
        this.cctx = cctx;
        this.name = name;
        this.hdrKey = new GridCacheMapHeaderKey(name);
        this.hdr = hdr;
        cache = cctx.kernalContext().cache().internalCache(cctx.name());
        compute = cctx.kernalContext().grid().compute();
    }

    /**
     * Returns the number of keys in this map
     */
    public long size() {
        try {
            return ((GridCacheMapHeader)cctx.kernalContext().cache()
                .internalCache(cctx.name()).get(hdrKey)).size();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * Clears the map. Removes all key-value pairs
     */
    @SuppressWarnings("unchecked")
    public void clear() {
        if (!collocated()) {
            try {
                cache.clear();
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }
        else {
            affinityRun(new IgniteRunnable() {
                @Override public void run() {
                    try {
                        for (Cache.Entry entry : GridCacheAbstractMapImpl.this.localEntries()) {
                            if (MapItemKey.class.isAssignableFrom(entry.getKey().getClass())) {
                                MapItemKey key = (MapItemKey)entry.getKey();

                                if (key.id().equals(GridCacheAbstractMapImpl.this.id()))
                                    cache.clearLocally(key);
                            }
                        }
                    }
                    catch (IgniteCheckedException e) {
                        throw U.convertException(e);
                    }
                }
            });
        }
    }

    /**
     * @return Iterable over local entries
     */
    protected Iterable<? extends Cache.Entry<?, ?>> localEntries() throws IgniteCheckedException {
        return cache.localEntries(new CachePeekMode[] {CachePeekMode.ALL});
    }

    /**
     * Executes given job on collocated map on the node where the map is located
     * (a.k.a. affinity co-location). This is not supported for non-collocated maps
     *
     * @param job Job which will be co-located with the map
     * @throws IgniteException If job failed
     */
    public void affinityRun(IgniteRunnable job) {
        if (!collocated())
            throw new IgniteException("Failed to execute affinityCall() for non-collocated datastructure: " + name +
                ". This operation is supported only for collocated datastructures.");

        compute.affinityRun(cache.name(), hdrKey, job);
    }

    /**
     * Executes given job on collocated map on the node where the map is located
     * (a.k.a. affinity co-location). This is not supported for non-collocated maps
     *
     * @param job Job which will be co-located with the map
     * @throws IgniteException If job failed
     */
    public <R> R affinityCall(IgniteCallable<R> job) {
        if (!collocated())
            throw new IgniteException("Failed to execute affinityCall() for non-collocated datastructure: " + name +
                ". This operation is supported only for collocated datastructures.");

        return compute.affinityCall(cache.name(), hdrKey, job);
    }

    /**
     * Modify multimap size by <tt>size</tt>
     *
     * @param size the value to change the size by
     * @throws IgniteCheckedException if any exception happens
     */
    protected void changeSize(int size) throws IgniteCheckedException {
        if (size != 0) {
            GridCacheAdapter<GridCacheMapHeaderKey, GridCacheMapHeader> cache0 =
                cctx.kernalContext().cache().internalCache(cctx.name());
            cache0.invoke(hdrKey, new SizeProcessor(id(), size));
        }
    }

    /**
     * Returns {@code true} if this map can be kept on the one node only
     * Returns {@code false} if this map can be kept on the many nodes
     *
     * @return {@code true} if this map is in {@code collocated} mode {@code false} otherwise
     */
    public abstract boolean collocated();

    /**
     * @return Map ID.
     */
    public abstract IgniteUuid id();

    /**
     */
    private static class SizeProcessor implements
        EntryProcessor<GridCacheMapHeaderKey, GridCacheMapHeader, Long>, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private IgniteUuid id;

        /** */
        private int size;

        /**
         * Required by {@link Externalizable}.
         */
        public SizeProcessor() {
            // No-op.
        }

        /**
         * @param id Map unique ID.
         * @param size Number of elements to add.
         */
        public SizeProcessor(IgniteUuid id, int size) {
            this.id = id;
            this.size = size;
        }

        /** {@inheritDoc} */
        @Override public Long process(MutableEntry<GridCacheMapHeaderKey, GridCacheMapHeader> e,
            Object... args) {
            GridCacheMapHeader hdr = e.getValue();

            if (removed(hdr, id)) {
                return MULTIMAP_REMOVED_IDX;
            }

            GridCacheMapHeader newHdr = new GridCacheMapHeader(hdr.id(),
                hdr.collocated(),
                hdr.size() + size);

            e.setValue(newHdr);

            return newHdr.size();
        }

        /**
         * @param hdr Map header.
         * @param id Expected map unique ID.
         * @return {@code True} if map was removed.
         */
        private boolean removed(@Nullable GridCacheMapHeader hdr, IgniteUuid id) {
            return hdr == null || !id.equals(hdr.id());
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeGridUuid(out, id);
            out.writeInt(size);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            id = U.readGridUuid(in);
            size = in.readInt();
        }
    }
}
