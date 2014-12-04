/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr.hub.sender.store.memory;

import org.gridgain.grid.*;
import org.gridgain.grid.dr.hub.sender.store.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.atomic.*;

import static org.gridgain.grid.dr.hub.sender.store.GridDrSenderHubStoreOverflowMode.*;

/**
 * Data center replication sender hub store implementation which stores data in memory.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * There are no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * The following configuration parameters are optional:
 * <ul>
 * <li>Maximum number of entries can be stored (see {@link #setMaxSize(int)})</li>
 * <li>Overflow mode defining how store will behave in case of overflow
 *      (see {@link #setOverflowMode(GridDrSenderHubStoreOverflowMode)})</li>
 * </ul>
 * <h2 class="header">Java Example</h2>
 * <pre name="code" class="java">
 * GridDrSenderHubConfiguration cfg = new GridDrSenderHubConfiguration();
 *
 * GridDrSenderHubInMemoryStore store = new GridDrSenderHubInMemoryStore();
 *
 * // Override default overflow mode.
 * store.setOverflowMode(GridDrSenderHubStoreOverflowMode.REMOVE_OLDEST);
 *
 * // Set in-memory store for sender hub.
 * cfg.setStore(store);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * GridDrSenderHubInMemoryStore can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.gridgain.grid.GridConfiguration" singleton="true"&gt;
 *         ...
 *         &lt;property name="drSenderHubConfiguration"&gt;
 *              &lt;bean class="org.gridgain.grid.dr.hub.sender.GridDrSenderHubConfiguration"&gt;
 *                  &lt;property name="store"&gt;
 *                      &lt;bean class="org.gridgain.grid.dr.hub.sender.store.memory.GridDrSenderHubInMemoryStore"&gt;
 *                          &lt;property name="overflowMode" value="REMOVE_OLDEST"/&gt;
 *                      &lt;/bean&gt;
 *                  &lt;/property&gt;
 *                 ...
 *              &lt;/bean&gt;
 *          &lt;/property&gt;
 *         ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://www.gridgain.com/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 * @see GridDrSenderHubStore
 */
public class GridDrSenderHubInMemoryStore implements GridDrSenderHubStore, GridLifecycleAware {
    /** */
    private static final int MAX_DATA_CENTERS = 32;

    /** */
    public static final int DFLT_MAX_SIZE = 8192;

    /** */
    public static final GridDrSenderHubStoreOverflowMode DFLT_OVERFLOW_MODE = STOP;

    /** */
    private int maxSize = DFLT_MAX_SIZE;

    /** */
    private GridDrSenderHubStoreOverflowMode overflowMode = DFLT_OVERFLOW_MODE;

    /** */
    private AtomicLong[] readIdxs;

    /** */
    private GridCircularBuffer<DrEntry> buf;

    /** */
    private IgniteInClosureX<DrEntry> evictC;

    /**
     * Gets maximum number of entries can be stored. This value should power of two.
     * <p>
     * Defaults to {@link #DFLT_MAX_SIZE}.
     *
     * @return Maximum number of entries in store.
     */
    public int getMaxSize() {
        return maxSize;
    }

    /**
     * Sets maximum number of entries can be stored. See {@link #getMaxSize()} for more information.
     *
     * @param maxSize Maximum number of entries in store.
     */
    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    /**
     * Gets overflow mode defining how store will behave in case of overflow.
     * <p>
     * Defaults to {@link #DFLT_OVERFLOW_MODE}.
     *
     * @return Overflow mode.
     */
    public GridDrSenderHubStoreOverflowMode getOverflowMode() {
        return overflowMode;
    }

    /**
     * Sets overflow mode defining how store will behave in case of overflow. See {@link #getOverflowMode()} for more
     * information.
     *
     * @param overflowMode Overflow mode.
     */
    public void setOverflowMode(GridDrSenderHubStoreOverflowMode overflowMode) {
        this.overflowMode = overflowMode;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        A.notNull(overflowMode, "overflowMode");
        A.ensure(maxSize > 0, "Maximum size should be greater than 0: " + maxSize);
        A.ensure((maxSize & (maxSize - 1)) == 0, "Maximum size should be power of two: " + maxSize);

        assert overflowMode == STOP || overflowMode == REMOVE_OLDEST;

        if (overflowMode == STOP) {
            evictC = new CIX1<DrEntry>() {
                @Override public void applyx(DrEntry drEntry) throws GridException {
                    if (!drEntry.readByAll())
                        throw new GridDrSenderHubStoreOverflowException();
                }
            };
        }

        buf = new GridCircularBuffer<>(maxSize);

        readIdxs = new AtomicLong[MAX_DATA_CENTERS];

        for (int i = 0; i < readIdxs.length; i++)
            readIdxs[i] = new AtomicLong();
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void store(byte[] dataCenterIds, byte[] data) throws GridException {
        try {
            buf.add(new DrEntry(dataCenterIds, data), evictC);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new GridInterruptedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public GridDrSenderHubStoreCursor cursor(byte dataCenterId) throws GridException {
        return new Cursor(dataCenterId);
    }

    /**
     *
     */
    private class Cursor implements GridDrSenderHubStoreCursor {
        /** */
        private final AtomicLong readIdx;

        /** */
        private int mask;

        /** */
        private long idx;

        /**
         * @param dataCenterId Data center ID.
         */
        private Cursor(byte dataCenterId) {
            readIdx = readIdxs[dataCenterId];

            idx = readIdx.get();

            mask = 1 << dataCenterId;
        }

        /** {@inheritDoc} */
        @Nullable @Override public GridDrSenderHubStoreEntry next() throws GridException {
            while (true) {
                long curIdx = idx;

                T2<DrEntry, Long> item = buf.get(curIdx);

                DrEntry drEntry = item.get1();

                if (drEntry == null)
                    return null;

                long itemIdx = item.get2();

                if (itemIdx < curIdx) // Already read this item.
                    return null;

                readIdx.set(++idx);

                if (itemIdx > curIdx) { // Old item was overwritten, switch to new items.
                    idx += (itemIdx - curIdx - maxSize);

                    continue;
                }

                if ((drEntry.mask & mask) != 0) {
                    final byte[] data = drEntry.read();

                    if (data == null)
                        continue;

                    return new GridDrSenderHubStoreEntry() {
                        @Override public byte[] data() {
                            return data;
                        }

                        @Override public void acknowledge() {
                            // No-op.
                        }
                    };
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Cursor.class, this);
        }
    }

    /**
     *
     */
    private static class DrEntry {
        /** */
        private final int mask;

        /** */
        private byte[] data;

        /** */
        private int cntr;

        /**
         * @param dataCenterIds Data center IDs.
         * @param data Data.
         */
        private DrEntry(byte[] dataCenterIds, byte[] data) {
            this.data = data;

            int mask0 = 0;

            for (byte dataCenterId : dataCenterIds)
                mask0 |= (1 << dataCenterId);

            mask = mask0;

            cntr = dataCenterIds.length;
        }

        /**
         * @return {@code True} is
         */
        synchronized boolean readByAll() {
            return data == null;
        }

        /**
         * @return Entry data.
         */
        synchronized byte[] read() {
            byte[] data0 = data;

            if (cntr > 0) {
                cntr--;

                if (cntr == 0)
                    data = null;
            }

            return data0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DrEntry.class, this);
        }
    }
}
