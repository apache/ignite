/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.IgniteConfiguration;
import org.gridgain.grid.cache.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMemoryMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 *
 */
public class GridCacheValueBytesPreloadingSelfTest extends GridCommonAbstractTest {
    /** Memory mode. */
    private GridCacheMemoryMode memMode;

    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheConfiguration(gridName));

        return cfg;
    }

    /**
     * @param gridName Grid name.
     * @return Cache configuration.
     * @throws Exception If failed.
     */
    protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration ccfg = new GridCacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setDistributionMode(PARTITIONED_ONLY);
        ccfg.setStoreValueBytes(true);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setMemoryMode(memMode);
        ccfg.setOffHeapMaxMemory(1024 * 1024 * 1024);
        ccfg.setPreloadMode(GridCachePreloadMode.SYNC);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testOnHeapTiered() throws Exception {
        memMode = ONHEAP_TIERED;

        startGrids(1);

        try {
            checkByteArrays();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testOffHeapTiered() throws Exception {
        memMode = OFFHEAP_TIERED;

        startGrids(1);

        try {
            checkByteArrays();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testOffHeapValuesOnly() throws Exception {
        memMode = OFFHEAP_VALUES;

        startGrids(1);

        try {
            checkByteArrays();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void checkByteArrays() throws Exception {
        int keyCnt = 1000;

        byte[] val = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};

        for (int i = 0; i < keyCnt; i++)
            grid(0).cache(null).put(String.valueOf(i), val);

        for (int i = 0; i < keyCnt; i++)
            grid(0).cache(null).get(String.valueOf(i));

        startGrid(1);

        if (memMode == ONHEAP_TIERED) {
            for (int i = 0; i < keyCnt; i++)
                grid(0).cache(null).evict(String.valueOf(i));

            for (int i = 0; i < keyCnt; i++)
                grid(0).cache(null).promote(String.valueOf(i));
        }

        startGrid(2);

        for (int g = 0; g < 3; g++) {
            for (int i = 0; i < keyCnt; i++) {
                byte[] o = (byte[])grid(g).cache(null).get(String.valueOf(i));

                assertTrue("Got invalid value [val=" + Arrays.toString(val) + ", actual=" + Arrays.toString(o) + ']',
                    Arrays.equals(val, o));
            }
        }
    }
}
