/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.store.local;

import org.gridgain.grid.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

/**
 * Striped thread safe version of {@link GridCacheFileLocalStoreMap} with support of {@code 0} key.
 * Also it applies supplemental hash function for any given key before storing it.
 *
 * @author @java.author
 * @version @java.version
 */
class GridCacheFileLocalStoreStripedMap implements AutoCloseable {
    /** Must be power of 2 length. */
    private final GridCacheFileLocalStoreMap[] segments;

    /** We need to have this shift to avoid collisions inside of segment (use other bits for mapping). */
    private final int shift;

    /** Values for key zero (zero has meaning of empty cell). */
    private final GridLongList zeroKeyVals = new GridLongList();

    /**
     * Initialize segments.
     * @param segmentsNum Number of segments. Must be power of 2.
     * @param segmentCap Capacity of a single segment. Must be power of 2.
     */
    GridCacheFileLocalStoreStripedMap(int segmentsNum, int segmentCap) {
        if (segmentCap == 0)
            segmentCap = 64;

        assert U.isPow2(segmentsNum) : "segmentsNum must be positive and power of 2: " + segmentsNum;
        assert U.isPow2(segmentCap) : "segmentCap must be power of 2: " + segmentCap;

        segments = new GridCacheFileLocalStoreMap[segmentsNum];
        shift = 32 - Integer.numberOfTrailingZeros(segmentsNum);

        for (int i = 0; i < segments.length; i++)
            segments[i] = new GridCacheFileLocalStoreMap(segmentCap);
    }

    /**
     * @param key Key.
     * @param val Value.
     */
    public void add(int key, long val) {
        assert (val >>> 48) == 0 : val;

        key = U.hash(key);

        if (key == 0) {
            synchronized (zeroKeyVals) {
                zeroKeyVals.add(val);
            }
        }
        else {
            GridCacheFileLocalStoreMap t = segment(key);

            synchronized (t) {
                t.add(key, val);
            }
        }
    }

    /**
     * @param key Key.
     * @param oldVal Old value.
     * @param newVal New value.
     * @return {@code true} If value was replaced.
     */
    public boolean replace(int key, long oldVal, long newVal) {
        assert (newVal >>> 48) == 0;

        key = U.hash(key);

        if (key == 0) {
            synchronized (zeroKeyVals) {
                return zeroKeyVals.replaceValue(0, oldVal, newVal) != -1;
            }
        }
        else {
            GridCacheFileLocalStoreMap t = segment(key);

            synchronized (t) {
                return t.replace(key, oldVal, newVal);
            }
        }
    }

    /**
     * @param key Key.
     * @param val Value.
     * @return {@code true} If entry was removed.
     */
    public boolean remove(int key, long val) {
        assert (val >>> 48) == 0 : val;

        key = U.hash(key);

        if (key == 0) {
            synchronized (zeroKeyVals) {
                return zeroKeyVals.removeValue(0, val) != -1;
            }
        }
        else {
            GridCacheFileLocalStoreMap t = segment(key);

            synchronized (t) {
                return t.remove(key, val);
            }
        }
    }

    /**
     * @param key Key.
     * @return List of results or {@code null} if none.
     */
    @Nullable public GridLongList get(int key) {
        key = U.hash(key);

        GridLongList res = null;

        if (key == 0) {
            synchronized (zeroKeyVals) {
                if (!zeroKeyVals.isEmpty()) {
                    res = new GridLongList();

                    res.addAll(zeroKeyVals);
                }
            }
        }
        else {
            GridCacheFileLocalStoreMap t = segment(key);

            synchronized (t) {
                res = t.get(key, res);
            }
        }

        return res;
    }

    /**
     * @param key Key.
     * @param val Value.
     * @return {@code true} If contains.
     */
    public boolean contains(int key, long val) {
        key = U.hash(key);

        if (key == 0) {
            synchronized (zeroKeyVals) {
                return zeroKeyVals.contains(val);
            }
        }
        else {
            GridCacheFileLocalStoreMap t = segment(key);

            synchronized (t) {
                return t.contains(key, val);
            }
        }
    }

    /**
     * @return Number of entries.
     */
    public int size() {
        int res = 0;

        synchronized (zeroKeyVals) {
            res += zeroKeyVals.size();
        }

        for (GridCacheFileLocalStoreMap t : segments) {
            synchronized (t) {
                res += t.size(true);
            }
        }

        return res;
    }

    /**
     * @param hash Hash.
     * @return Segment.
     */
    private GridCacheFileLocalStoreMap segment(int hash) {
        return segments[hash >>> shift]; // Taking needed number of most significant bits.
    }

    /**
     * Applies given closure for each pair.
     *
     * @param c Closure.
     * @throws GridException If failed.
     */
    public void iterate(GridCacheFileLocalStoreMap.Closure c) throws GridException {
        synchronized (zeroKeyVals) {
            for (int i = 0; i < zeroKeyVals.size(); i++)
                c.apply(zeroKeyVals.get(i));
        }

        for (GridCacheFileLocalStoreMap t : segments) {
            synchronized (t) {
                t.iterate(c);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        for (GridCacheFileLocalStoreMap t : segments) {
            synchronized (t) {
                t.close();
            }
        }
    }
}
