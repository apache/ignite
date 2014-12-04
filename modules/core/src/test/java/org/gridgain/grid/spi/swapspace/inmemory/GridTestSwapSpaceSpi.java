/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.swapspace.inmemory;

import org.apache.ignite.lang.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.swapspace.*;
import org.gridgain.grid.util.typedef.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.events.GridEventType.*;

/**
 * Test swap space SPI that stores values in map.
 */
@GridSpiMultipleInstancesSupport(true)
public class GridTestSwapSpaceSpi extends GridSpiAdapter implements GridSwapSpaceSpi {
    /** Listener. */
    private GridSwapSpaceSpiListener lsnr;

    /** Spaces map. */
    private ConcurrentMap<String, Space> spaces = new ConcurrentHashMap8<>();

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String gridName) throws GridSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws GridSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void clear(@Nullable String spaceName) throws GridSpiException {
        Space space = space(spaceName);

        if (space != null)
            space.clear();
    }

    /** {@inheritDoc} */
    @Override public long size(@Nullable String spaceName) throws GridSpiException {
        Space space = space(spaceName);

        return space != null ? space.size() : 0;
    }

    /** {@inheritDoc} */
    @Override public long count(@Nullable String spaceName) throws GridSpiException {
        Space space = space(spaceName);

        return space != null ? space.count() : 0;
    }

    /** {@inheritDoc} */
    @Override public byte[] read(@Nullable String spaceName, GridSwapKey key, GridSwapContext ctx)
        throws GridSpiException {
        Space space = space(spaceName);

        return space != null ? space.read(key) : null;
    }

    /** {@inheritDoc} */
    @Override public Map<GridSwapKey, byte[]> readAll(@Nullable String spaceName, Iterable<GridSwapKey> keys,
        GridSwapContext ctx) throws GridSpiException {
        Space space = space(spaceName);

        return space != null ? space.readAll(keys) : Collections.<GridSwapKey, byte[]>emptyMap();
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable String spaceName, GridSwapKey key, @Nullable IgniteInClosure<byte[]> c,
        GridSwapContext ctx) throws GridSpiException {
        Space space = space(spaceName);

        if (space != null)
            space.remove(key, c);
    }

    /** {@inheritDoc} */
    @Override public void removeAll(@Nullable String spaceName, Collection<GridSwapKey> keys,
        @Nullable IgniteBiInClosure<GridSwapKey, byte[]> c, GridSwapContext ctx) throws GridSpiException {
        Space space = space(spaceName);

        if (space != null)
            space.removeAll(keys, c);
    }

    /** {@inheritDoc} */
    @Override public void store(@Nullable String spaceName, GridSwapKey key, @Nullable byte[] val, GridSwapContext ctx)
        throws GridSpiException {
        ensureSpace(spaceName).store(key, val);
    }

    /** {@inheritDoc} */
    @Override public void storeAll(@Nullable String spaceName, Map<GridSwapKey, byte[]> pairs, GridSwapContext ctx)
        throws GridSpiException {
        ensureSpace(spaceName).storeAll(pairs);
    }

    /** {@inheritDoc} */
    @Override public void setListener(@Nullable GridSwapSpaceSpiListener evictLsnr) {
        lsnr = evictLsnr;
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> partitions(@Nullable String spaceName) throws GridSpiException {
        Space space = space(spaceName);

        return space != null ? space.partitions() : Collections.<Integer>emptyList();
    }

    /** {@inheritDoc} */
    @Override public <K> GridSpiCloseableIterator<K> keyIterator(@Nullable String spaceName, GridSwapContext ctx)
        throws GridSpiException {
        return ensureSpace(spaceName).keyIterator();
    }

    /** {@inheritDoc} */
    @Override public GridSpiCloseableIterator<Map.Entry<byte[], byte[]>> rawIterator(@Nullable String spaceName)
        throws GridSpiException {
        return ensureSpace(spaceName).rawIterator();
    }

    /** {@inheritDoc} */
    @Override public GridSpiCloseableIterator<Map.Entry<byte[], byte[]>> rawIterator(@Nullable String spaceName, int part)
        throws GridSpiException {
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

    private void fireEvent(int evtType, String spaceName, @Nullable byte[] key) {
        GridSwapSpaceSpiListener lsnr0 = lsnr;

        if (lsnr0 != null)
            lsnr0.onSwapEvent(evtType, spaceName, key);
    }

    private class Space {
        /** Data storage. */
        private ConcurrentMap<GridSwapKey, byte[]> data = new ConcurrentHashMap8<>();

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
         * @param key Key to read.
         * @return Read bytes.
         */
        public byte[] read(GridSwapKey key) {
            byte[] bytes = data.get(key);

            fireEvent(EVT_SWAP_SPACE_DATA_READ, name, key.keyBytes());

            return bytes;
        }

        /**
         * @param keys Keys to read.
         * @return Read keys.
         */
        public Map<GridSwapKey, byte[]> readAll(Iterable<GridSwapKey> keys) {
            Map<GridSwapKey, byte[]> res = new HashMap<>();

            for (GridSwapKey key : keys) {
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
        public void remove(GridSwapKey key, IgniteInClosure<byte[]> c) {
            byte[] val = data.remove(key);

            if (val != null) {
                c.apply(val);

                fireEvent(EVT_SWAP_SPACE_DATA_REMOVED, name, key.keyBytes());
            }
        }

        /**
         * @param keys Keys to remove.
         * @param c Closure to apply for removed values.
         */
        public void removeAll(Iterable<GridSwapKey> keys, IgniteBiInClosure<GridSwapKey, byte[]> c) {
            for (GridSwapKey key : keys) {
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
        public void store(GridSwapKey key, byte[] val) {
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
        public void storeAll(Map<GridSwapKey, byte[]> pairs) {
            for (Map.Entry<GridSwapKey, byte[]> entry : pairs.entrySet()) {
                GridSwapKey key = entry.getKey();
                byte[] val = entry.getValue();

                store(key, val);
            }
        }

        /**
         * @return Partitions in space.
         */
        public Collection<Integer> partitions() {
            Collection<Integer> parts = new HashSet<>();

            for (GridSwapKey key : data.keySet())
                parts.add(key.partition());

            return parts;
        }

        public <K> GridSpiCloseableIterator<K> keyIterator() {
            final Iterator<GridSwapKey> it = data.keySet().iterator();

            return new GridSpiCloseableIterator<K>() {
                @Override public void close() {
                    // No-op.
                }

                @Override public boolean hasNext() {
                    return it.hasNext();
                }

                @Override public K next() {
                    GridSwapKey next = it.next();

                    return (K)next.key();
                }

                @Override public void remove() {
                    it.remove();
                }
            };
        }

        public GridSpiCloseableIterator<Map.Entry<byte[], byte[]>> rawIterator() {
            final Iterator<Map.Entry<GridSwapKey, byte[]>> it = data.entrySet().iterator();

            return new GridSpiCloseableIterator<Map.Entry<byte[], byte[]>>() {
                @Override public void close() {
                    // No-op.
                }

                @Override public boolean hasNext() {
                    return it.hasNext();
                }

                @Override public Map.Entry<byte[], byte[]> next() {
                    final Map.Entry<GridSwapKey, byte[]> next = it.next();

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

        public GridSpiCloseableIterator<Map.Entry<byte[], byte[]>> rawIterator(final int part) {
            final Iterator<Map.Entry<GridSwapKey, byte[]>> it = data.entrySet().iterator();

            return new GridSpiCloseableIterator<Map.Entry<byte[], byte[]>>() {
                /** Next entry in this iterator. */
                private Map.Entry<GridSwapKey, byte[]> next;

                private Map.Entry<GridSwapKey, byte[]> cur;

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

                    final Map.Entry<GridSwapKey, byte[]> ret = next;

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
                        Map.Entry<GridSwapKey, byte[]> entry = it.next();

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
