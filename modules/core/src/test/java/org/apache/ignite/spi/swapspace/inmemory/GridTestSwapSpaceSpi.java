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

package org.apache.ignite.spi.swapspace.inmemory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiCloseableIterator;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.swapspace.SwapContext;
import org.apache.ignite.spi.swapspace.SwapKey;
import org.apache.ignite.spi.swapspace.SwapSpaceSpi;
import org.apache.ignite.spi.swapspace.SwapSpaceSpiListener;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.events.EventType.EVT_SWAP_SPACE_CLEARED;
import static org.apache.ignite.events.EventType.EVT_SWAP_SPACE_DATA_READ;
import static org.apache.ignite.events.EventType.EVT_SWAP_SPACE_DATA_REMOVED;
import static org.apache.ignite.events.EventType.EVT_SWAP_SPACE_DATA_STORED;

/**
 * Test swap space SPI that stores values in map.
 */
@IgniteSpiMultipleInstancesSupport(true)
public class GridTestSwapSpaceSpi extends IgniteSpiAdapter implements SwapSpaceSpi {
    /** Listener. */
    private SwapSpaceSpiListener lsnr;

    /** Spaces map. */
    private ConcurrentMap<String, Space> spaces = new ConcurrentHashMap8<>();

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String gridName) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void clear(@Nullable String spaceName) throws IgniteSpiException {
        Space space = space(spaceName);

        if (space != null)
            space.clear();
    }

    /** {@inheritDoc} */
    @Override public long size(@Nullable String spaceName) throws IgniteSpiException {
        Space space = space(spaceName);

        return space != null ? space.size() : 0;
    }

    /** {@inheritDoc} */
    @Override public long count(@Nullable String spaceName) throws IgniteSpiException {
        Space space = space(spaceName);

        return space != null ? space.count() : 0;
    }

    /** {@inheritDoc} */
    @Override public long count(@Nullable String spaceName, Set<Integer> parts) throws IgniteSpiException {
        Space space = space(spaceName);

        return space != null ? space.count(parts) : 0;
    }

    /** {@inheritDoc} */
    @Override public byte[] read(@Nullable String spaceName, SwapKey key, SwapContext ctx)
        throws IgniteSpiException {
        Space space = space(spaceName);

        return space != null ? space.read(key) : null;
    }

    /** {@inheritDoc} */
    @Override public Map<SwapKey, byte[]> readAll(@Nullable String spaceName, Iterable<SwapKey> keys,
        SwapContext ctx) throws IgniteSpiException {
        Space space = space(spaceName);

        return space != null ? space.readAll(keys) : Collections.<SwapKey, byte[]>emptyMap();
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable String spaceName, SwapKey key, @Nullable IgniteInClosure<byte[]> c,
        SwapContext ctx) throws IgniteSpiException {
        Space space = space(spaceName);

        if (space != null)
            space.remove(key, c);
    }

    /** {@inheritDoc} */
    @Override public void removeAll(@Nullable String spaceName, Collection<SwapKey> keys,
        @Nullable IgniteBiInClosure<SwapKey, byte[]> c, SwapContext ctx) throws IgniteSpiException {
        Space space = space(spaceName);

        if (space != null)
            space.removeAll(keys, c);
    }

    /** {@inheritDoc} */
    @Override public void store(@Nullable String spaceName, SwapKey key, @Nullable byte[] val, SwapContext ctx)
        throws IgniteSpiException {
        ensureSpace(spaceName).store(key, val);
    }

    /** {@inheritDoc} */
    @Override public void storeAll(@Nullable String spaceName, Map<SwapKey, byte[]> pairs, SwapContext ctx)
        throws IgniteSpiException {
        ensureSpace(spaceName).storeAll(pairs);
    }

    /** {@inheritDoc} */
    @Override public void setListener(@Nullable SwapSpaceSpiListener evictLsnr) {
        lsnr = evictLsnr;
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> partitions(@Nullable String spaceName) throws IgniteSpiException {
        Space space = space(spaceName);

        return space != null ? space.partitions() : Collections.<Integer>emptyList();
    }

    /** {@inheritDoc} */
    @Override public <K> IgniteSpiCloseableIterator<K> keyIterator(@Nullable String spaceName, SwapContext ctx)
        throws IgniteSpiException {
        return ensureSpace(spaceName).keyIterator();
    }

    /** {@inheritDoc} */
    @Override public IgniteSpiCloseableIterator<Map.Entry<byte[], byte[]>> rawIterator(@Nullable String spaceName)
        throws IgniteSpiException {
        return ensureSpace(spaceName).rawIterator();
    }

    /** {@inheritDoc} */
    @Override public IgniteSpiCloseableIterator<Map.Entry<byte[], byte[]>> rawIterator(@Nullable String spaceName, int part)
        throws IgniteSpiException {
        return ensureSpace(spaceName).rawIterator(part);
    }

    /**
     * @param spaceName Space name.
     * @return Space object.
     */
    @Nullable private Space space(String spaceName) {
        return spaces.get(spaceName);
    }

    /**
     * Gets space, creates if does not exist.
     *
     * @param spaceName Space name.
     * @return Space.
     */
    private Space ensureSpace(String spaceName) {
        Space space = spaces.get(spaceName);

        if (space == null)
            space = F.addIfAbsent(spaces, spaceName, new Space(spaceName));

        return space;
    }

    /**
     * @param evtType Event type.
     * @param spaceName Space name.
     * @param key Key bytes.
     */
    private void fireEvent(int evtType, String spaceName, @Nullable byte[] key) {
        SwapSpaceSpiListener lsnr0 = lsnr;

        if (lsnr0 != null)
            lsnr0.onSwapEvent(evtType, spaceName, key, null);
    }

    /**
     *
     */
    private class Space {
        /** Data storage. */
        private ConcurrentMap<SwapKey, byte[]> data = new ConcurrentHashMap8<>();

        /** */
        private final String name;

        /**
         * @param name Space name.
         */
        private Space(String name) {
            this.name = name;
        }

        /**
         * Clears space.
         */
        public void clear() {
            data.clear();

            fireEvent(EVT_SWAP_SPACE_CLEARED, name, null);
        }

        /**
         * @return Space size.
         */
        public long size() {
            return data.size();
        }

        /**
         * @return Space size.
         */
        public long count() {
            return data.size();
        }

        /**
         * @param parts Partitions.
         * @return Number of entries for given partitions.
         */
        public long count(Set<Integer> parts) {
            int cnt = 0;

            for (SwapKey key : data.keySet()) {
                if (parts.contains(key.partition()))
                    cnt++;
            }

            return cnt;
        }

        /**
         * @param key Key to read.
         * @return Read bytes.
         */
        public byte[] read(SwapKey key) {
            byte[] bytes = data.get(key);

            fireEvent(EVT_SWAP_SPACE_DATA_READ, name, key.keyBytes());

            return bytes;
        }

        /**
         * @param keys Keys to read.
         * @return Read keys.
         */
        public Map<SwapKey, byte[]> readAll(Iterable<SwapKey> keys) {
            Map<SwapKey, byte[]> res = new HashMap<>();

            for (SwapKey key : keys) {
                byte[] val = data.get(key);

                if (val != null) {
                    res.put(key, val);

                    fireEvent(EVT_SWAP_SPACE_DATA_READ, name, key.keyBytes());
                }
            }

            return res;
        }

        /**
         * @param key Key to remove.
         * @param c Closure.
         */
        public void remove(SwapKey key, IgniteInClosure<byte[]> c) {
            byte[] val = data.remove(key);

            if (val != null) {
                if (c != null)
                    c.apply(val);

                fireEvent(EVT_SWAP_SPACE_DATA_REMOVED, name, key.keyBytes());
            }
        }

        /**
         * @param keys Keys to remove.
         * @param c Closure to apply for removed values.
         */
        public void removeAll(Iterable<SwapKey> keys, IgniteBiInClosure<SwapKey, byte[]> c) {
            for (SwapKey key : keys) {
                byte[] val = data.remove(key);

                if (val != null) {
                    c.apply(key, val);

                    fireEvent(EVT_SWAP_SPACE_DATA_REMOVED, name, key.keyBytes());
                }
            }
        }

        /**
         * @param key Key to store.
         * @param val Value to store.
         */
        public void store(SwapKey key, byte[] val) {
            if (val != null) {
                data.put(key, val);

                fireEvent(EVT_SWAP_SPACE_DATA_STORED, name, key.keyBytes());
            }
            else {
                val = data.remove(key);

                if (val != null)
                    fireEvent(EVT_SWAP_SPACE_DATA_REMOVED, name, key.keyBytes());
            }
        }

        /**
         * @param pairs Values to store.
         */
        public void storeAll(Map<SwapKey, byte[]> pairs) {
            for (Map.Entry<SwapKey, byte[]> entry : pairs.entrySet()) {
                SwapKey key = entry.getKey();
                byte[] val = entry.getValue();

                store(key, val);
            }
        }

        /**
         * @return Partitions in space.
         */
        public Collection<Integer> partitions() {
            Collection<Integer> parts = new HashSet<>();

            for (SwapKey key : data.keySet())
                parts.add(key.partition());

            return parts;
        }

        /**
         * @return Iterator.
         */
        public <K> IgniteSpiCloseableIterator<K> keyIterator() {
            final Iterator<SwapKey> it = data.keySet().iterator();

            return new IgniteSpiCloseableIterator<K>() {
                @Override public void close() {
                    // No-op.
                }

                @Override public boolean hasNext() {
                    return it.hasNext();
                }

                @Override public K next() {
                    SwapKey next = it.next();

                    return (K)next.key();
                }

                @Override public void remove() {
                    it.remove();
                }
            };
        }

        /**
         * @return Raw iterator.
         */
        public IgniteSpiCloseableIterator<Map.Entry<byte[], byte[]>> rawIterator() {
            final Iterator<Map.Entry<SwapKey, byte[]>> it = data.entrySet().iterator();

            return new IgniteSpiCloseableIterator<Map.Entry<byte[], byte[]>>() {
                @Override public void close() {
                    // No-op.
                }

                @Override public boolean hasNext() {
                    return it.hasNext();
                }

                @Override public Map.Entry<byte[], byte[]> next() {
                    final Map.Entry<SwapKey, byte[]> next = it.next();

                    return new Map.Entry<byte[], byte[]>() {
                        @Override public byte[] getKey() {
                            return next.getKey().keyBytes();
                        }

                        @Override public byte[] getValue() {
                            return next.getValue();
                        }

                        @Override public byte[] setValue(byte[] val) {
                            return data.put(next.getKey(), val);
                        }
                    };
                }

                @Override public void remove() {
                    it.remove();
                }
            };
        }

        /**
         * @param part Partition.
         * @return Raw iterator for partition.
         */
        public IgniteSpiCloseableIterator<Map.Entry<byte[], byte[]>> rawIterator(final int part) {
            final Iterator<Map.Entry<SwapKey, byte[]>> it = data.entrySet().iterator();

            return new IgniteSpiCloseableIterator<Map.Entry<byte[], byte[]>>() {
                /** Next entry in this iterator. */
                private Map.Entry<SwapKey, byte[]> next;

                private Map.Entry<SwapKey, byte[]> cur;

                {
                    advance();
                }

                @Override public void close() {
                    // No-op.
                }

                @Override public boolean hasNext() {
                    return next != null;
                }

                @Override public Map.Entry<byte[], byte[]> next() {
                    if (next == null)
                        throw new NoSuchElementException();

                    final Map.Entry<SwapKey, byte[]> ret = next;

                    cur = ret;

                    advance();

                    return new Map.Entry<byte[], byte[]>() {
                        @Override public byte[] getKey() {
                            return ret.getKey().keyBytes();
                        }

                        @Override public byte[] getValue() {
                            return ret.getValue();
                        }

                        @Override public byte[] setValue(byte[] val) {
                            return data.put(ret.getKey(), val);
                        }
                    };
                }

                @Override public void remove() {
                    if (cur == null)
                        throw new IllegalStateException();

                    data.remove(cur.getKey(), cur.getValue());
                }

                private void advance() {
                    while (it.hasNext()) {
                        Map.Entry<SwapKey, byte[]> entry = it.next();

                        if(entry.getKey().partition() == part) {
                            cur = next;

                            next = entry;

                            return;
                        }
                    }

                    next = null;
                }
            };
        }
    }
}