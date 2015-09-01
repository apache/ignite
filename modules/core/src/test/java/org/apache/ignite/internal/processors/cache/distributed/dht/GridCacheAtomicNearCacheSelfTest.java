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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.CLOCK;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_GRID_NAME;

/**
 * Tests near cache with various atomic cache configuration.
 */
public class GridCacheAtomicNearCacheSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 4;

    /** */
    private static final int PRIMARY = 0;

    /** */
    private static final int BACKUP = 1;

    /** */
    private static final int NOT_PRIMARY_AND_BACKUP = 2;

    /** */
    private int backups;

    /** */
    private CacheAtomicWriteOrderMode writeOrderMode;

    /** */
    private int lastKey;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setNearConfiguration(new NearCacheConfiguration());
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setRebalanceMode(SYNC);

        assert writeOrderMode != null;

        ccfg.setAtomicWriteOrderMode(writeOrderMode);
        ccfg.setBackups(backups);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoBackupsPrimaryWriteOrder() throws Exception {
        startGrids(0, CacheAtomicWriteOrderMode.PRIMARY);

        checkNearCache();
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoBackupsClockWriteOrder() throws Exception {
        startGrids(0, CLOCK);

        checkNearCache();
    }

    /**
     * @throws Exception If failed.
     */
    public void testWithBackupsPrimaryWriteOrder() throws Exception {
        startGrids(2, CacheAtomicWriteOrderMode.PRIMARY);

        checkNearCache();
    }


    /**
     * @throws Exception If failed.
     */
    public void testWithBackupsClockWriteOrder() throws Exception {
        startGrids(2, CLOCK);

        checkNearCache();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    private void checkNearCache() throws Exception {
        checkPut();

        checkPutAll();

        checkTransform();

        checkTransformAll();

        checkRemove();

        checkReaderEvict();

        checkReaderRemove();

        checkPutRemoveGet();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    private void checkPutAll() throws Exception {
        log.info("Check putAll.");

        Ignite ignite0 = grid(0);

        IgniteCache<Integer, Integer> cache0 = ignite0.cache(null);

        Affinity<Integer> aff = ignite0.affinity(null);

        UUID id0 = ignite0.cluster().localNode().id();

        Map<Integer, Integer> primaryKeys = new HashMap<>();

        for (int i = 0; i < 10; i++)
            primaryKeys.put(key(ignite0, PRIMARY), 1);

        log.info("PutAll from primary.");

        cache0.putAll(primaryKeys);

        for (int i = 0; i < GRID_CNT; i++) {
            for (Integer primaryKey : primaryKeys.keySet())
                checkEntry(grid(i), primaryKey, 1, false);
        }

        if (backups > 0) {
            Map<Integer, Integer> backupKeys = new HashMap<>();

            for (int i = 0; i < 10; i++)
                backupKeys.put(key(ignite0, BACKUP), 2);

            log.info("PutAll from backup.");

            cache0.putAll(backupKeys);

            for (int i = 0; i < GRID_CNT; i++) {
                for (Integer backupKey : backupKeys.keySet())
                    checkEntry(grid(i), backupKey, 2, false);
            }
        }

        Map<Integer, Integer> nearKeys = new HashMap<>();

        for (int i = 0; i < 30; i++)
            nearKeys.put(key(ignite0, NOT_PRIMARY_AND_BACKUP), 3);

        log.info("PutAll from near.");

        cache0.putAll(nearKeys);

        for (int i = 0; i < GRID_CNT; i++) {
            for (Integer nearKey : nearKeys.keySet()) {
                UUID[] expReaders = aff.isPrimary(grid(i).localNode(), nearKey) ? new UUID[]{id0} : new UUID[]{};

                checkEntry(grid(i), nearKey, 3, i == 0, expReaders);
            }
        }

        Map<Integer, Collection<UUID>> readersMap = new HashMap<>();

        for (Integer key : nearKeys.keySet())
            readersMap.put(key, new HashSet<UUID>());

        int val = 4;

        for (int i = 0; i < GRID_CNT; i++) {
            IgniteCache<Integer, Integer> cache = grid(i).cache(null);

            atomicClockModeDelay(cache);

            for (Integer key : nearKeys.keySet())
                nearKeys.put(key, val);

            log.info("PutAll [grid=" + grid(i).name() + ", val=" + val + ']');

            cache.putAll(nearKeys);

            for (Integer key : nearKeys.keySet()) {
                if (!aff.isPrimaryOrBackup(grid(i).localNode(), key))
                    readersMap.get(key).add(grid(i).localNode().id());
            }

            for (int j = 0; j < GRID_CNT; j++) {
                for (Integer key : nearKeys.keySet()) {
                    boolean primaryNode = aff.isPrimary(grid(j).localNode(), key);

                    Collection<UUID> readers = readersMap.get(key);

                    UUID[] expReaders = primaryNode ? U.toArray(readers, new UUID[readers.size()]) : new UUID[]{};

                    checkEntry(grid(j), key, val, readers.contains(grid(j).localNode().id()), expReaders);
                }
            }

            val++;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    private void checkTransform() throws Exception {
        log.info("Check transform.");

        Ignite ignite0 = grid(0);

        IgniteCache<Integer, Integer> cache0 = ignite0.cache(null);

        Affinity<Object> aff = ignite0.affinity(null);

        UUID id0 = ignite0.cluster().localNode().id();

        Integer primaryKey = key(ignite0, PRIMARY);

        log.info("Transform from primary.");

        cache0.invoke(primaryKey, new Processor(primaryKey));

        for (int i = 0; i < GRID_CNT; i++)
            checkEntry(grid(i), primaryKey, primaryKey, false);

        if (backups > 0) {
            Integer backupKey = key(ignite0, BACKUP);

            log.info("Transform from backup.");

            cache0.invoke(backupKey, new Processor(backupKey));

            for (int i = 0; i < GRID_CNT; i++)
                checkEntry(grid(i), backupKey, backupKey, false);
        }

        Integer nearKey = key(ignite0, NOT_PRIMARY_AND_BACKUP);

        log.info("Transform from near.");

        cache0.invoke(nearKey, new Processor(nearKey));

        for (int i = 0; i < GRID_CNT; i++) {
            UUID[] expReaders = aff.isPrimary(grid(i).localNode(), nearKey) ? new UUID[]{id0} : new UUID[]{};

            checkEntry(grid(i), nearKey, nearKey, i == 0, expReaders);
        }

        Collection<UUID> readers = new HashSet<>();

        readers.add(id0);

        int val = nearKey + 1;

        for (int i = 0; i < GRID_CNT; i++) {
            IgniteCache<Integer, Integer> cache = grid(i).cache(null);

            atomicClockModeDelay(cache);

            log.info("Transform [grid=" + grid(i).name() + ", val=" + val + ']');

            cache.invoke(nearKey, new Processor(val));

            if (!aff.isPrimaryOrBackup(grid(i).localNode(), nearKey))
                readers.add(grid(i).localNode().id());

            for (int j = 0; j < GRID_CNT; j++) {
                boolean primaryNode = aff.isPrimary(grid(j).localNode(), nearKey);

                UUID[] expReaders = primaryNode ? U.toArray(readers, new UUID[readers.size()]) : new UUID[]{};

                checkEntry(grid(j), nearKey, val, readers.contains(grid(j).localNode().id()), expReaders);
            }

            val++;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    private void checkTransformAll() throws Exception {
        log.info("Check transformAll.");

        Ignite ignite0 = grid(0);

        IgniteCache<Integer, Integer> cache0 = ignite0.cache(null);

        Affinity<Object> aff = ignite0.affinity(null);

        UUID id0 = ignite0.cluster().localNode().id();

        Set<Integer> primaryKeys = new HashSet<>();

        for (int i = 0; i < 10; i++)
            primaryKeys.add(key(ignite0, PRIMARY));

        log.info("TransformAll from primary.");

        cache0.invokeAll(primaryKeys, new Processor(1));

        for (int i = 0; i < GRID_CNT; i++) {
            for (Integer primaryKey : primaryKeys)
                checkEntry(grid(i), primaryKey, 1, false);
        }

        if (backups > 0) {
            Set<Integer> backupKeys = new HashSet<>();

            for (int i = 0; i < 10; i++)
                backupKeys.add(key(ignite0, BACKUP));

            log.info("TransformAll from backup.");

            cache0.invokeAll(backupKeys, new Processor(2));

            for (int i = 0; i < GRID_CNT; i++) {
                for (Integer backupKey : backupKeys)
                    checkEntry(grid(i), backupKey, 2, false);
            }
        }

        Set<Integer> nearKeys = new HashSet<>();

        for (int i = 0; i < 30; i++)
            nearKeys.add(key(ignite0, NOT_PRIMARY_AND_BACKUP));

        log.info("TransformAll from near.");

        cache0.invokeAll(nearKeys, new Processor(3));

        for (int i = 0; i < GRID_CNT; i++) {
            for (Integer nearKey : nearKeys) {
                UUID[] expReaders = aff.isPrimary(grid(i).localNode(), nearKey) ? new UUID[]{id0} : new UUID[]{};

                checkEntry(grid(i), nearKey, 3, i == 0, expReaders);
            }
        }

        Map<Integer, Collection<UUID>> readersMap = new HashMap<>();

        for (Integer key : nearKeys)
            readersMap.put(key, new HashSet<UUID>());

        int val = 4;

        for (int i = 0; i < GRID_CNT; i++) {
            IgniteCache<Integer, Integer> cache = grid(i).cache(null);

            atomicClockModeDelay(cache);

            for (Integer key : nearKeys)
                nearKeys.add(key);

            log.info("TransformAll [grid=" + grid(i).name() + ", val=" + val + ']');

            cache.invokeAll(nearKeys, new Processor(val));

            for (Integer key : nearKeys) {
                if (!aff.isPrimaryOrBackup(grid(i).localNode(), key))
                    readersMap.get(key).add(grid(i).localNode().id());
            }

            for (int j = 0; j < GRID_CNT; j++) {
                for (Integer key : nearKeys) {
                    boolean primaryNode = aff.isPrimary(grid(j).localNode(), key);

                    Collection<UUID> readers = readersMap.get(key);

                    UUID[] expReaders = primaryNode ? U.toArray(readers, new UUID[readers.size()]) : new UUID[]{};

                    checkEntry(grid(j), key, val, readers.contains(grid(j).localNode().id()), expReaders);
                }
            }

            val++;
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void checkPutRemoveGet() throws Exception {
        Ignite ignite0 = grid(0);

        IgniteCache<Integer, Integer> cache0 = ignite0.cache(null);

        Integer key = key(ignite0, NOT_PRIMARY_AND_BACKUP);

        cache0.put(key, key);

        for (int i = 0; i < GRID_CNT; i++)
            grid(i).cache(null).get(key);

        cache0.remove(key);

        cache0.put(key, key);

        for (int i = 0; i < GRID_CNT; i++)
            grid(i).cache(null).get(key);

        cache0.remove(key);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkPut() throws Exception {
        checkPut(0);

        checkPut(GRID_CNT - 1);
    }

    /**
     * @param grid Grid Index.
     * @throws Exception If failed.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    private void checkPut(int grid) throws Exception {
        log.info("Check put, grid: " + grid);

        Ignite ignite0 = grid(grid);

        IgniteCache<Integer, Integer> cache0 = ignite0.cache(null);

        Affinity<Integer> aff = ignite0.affinity(null);

        UUID id0 = ignite0.cluster().localNode().id();

        Integer primaryKey = key(ignite0, PRIMARY);

        log.info("Put from primary.");

        cache0.put(primaryKey, primaryKey);

        for (int i = 0; i < GRID_CNT; i++)
            checkEntry(grid(i), primaryKey, primaryKey, false);

        if (backups > 0) {
            Integer backupKey = key(ignite0, BACKUP);

            log.info("Put from backup.");

            cache0.put(backupKey, backupKey);

            for (int i = 0; i < GRID_CNT; i++)
                checkEntry(grid(i), backupKey, backupKey, false);
        }

        Integer nearKey = key(ignite0, NOT_PRIMARY_AND_BACKUP);

        log.info("Put from near.");

        cache0.put(nearKey, nearKey);

        for (int i = 0; i < GRID_CNT; i++) {
            UUID[] expReaders = aff.isPrimary(grid(i).localNode(), nearKey) ? new UUID[]{id0} : new UUID[]{};

            checkEntry(grid(i), nearKey, nearKey, i == grid, expReaders);
        }

        Collection<UUID> readers = new HashSet<>();

        readers.add(id0);

        int val = nearKey + 1;

        for (int i = 0; i < GRID_CNT; i++) {
            IgniteCache<Integer, Integer> cache = grid(i).cache(null);

            atomicClockModeDelay(cache);

            log.info("Put [grid=" + grid(i).name() + ", val=" + val + ']');

            cache.put(nearKey, val);

            if (!aff.isPrimaryOrBackup(grid(i).localNode(), nearKey))
                readers.add(grid(i).localNode().id());

            for (int j = 0; j < GRID_CNT; j++) {
                boolean primaryNode = aff.isPrimary(grid(j).localNode(), nearKey);

                UUID[] expReaders = primaryNode ? U.toArray(readers, new UUID[readers.size()]) : new UUID[]{};

                checkEntry(grid(j), nearKey, val, readers.contains(grid(j).localNode().id()), expReaders);
            }

            val++;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    private void checkRemove() throws Exception {
        log.info("Check remove.");

        Ignite ignite0 = grid(0);

        IgniteCache<Integer, Integer> cache0 = ignite0.cache(null);

        Integer primaryKey = key(ignite0, PRIMARY);

        log.info("Put from primary.");

        cache0.put(primaryKey, primaryKey);

        for (int i = 0; i < GRID_CNT; i++)
            checkEntry(grid(i), primaryKey, primaryKey, false);

        log.info("Remove from primary.");

        cache0.remove(primaryKey);

        for (int i = 0; i < GRID_CNT; i++)
            checkEntry(grid(i), primaryKey, null, false);

        if (backups > 0) {
            Integer backupKey = key(ignite0, BACKUP);

            log.info("Put from backup.");

            cache0.put(backupKey, backupKey);

            for (int i = 0; i < GRID_CNT; i++)
                checkEntry(grid(i), backupKey, backupKey, false);

            log.info("Remove from backup.");

            cache0.remove(backupKey);

            for (int i = 0; i < GRID_CNT; i++)
                checkEntry(grid(i), backupKey, null, false);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    private void checkReaderEvict() throws Exception {
        log.info("Check evict.");

        Ignite ignite0 = grid(0);

        IgniteCache<Integer, Integer> cache0 = ignite0.cache(null);

        Affinity<Integer> aff = ignite0.affinity(null);

        UUID id0 = ignite0.cluster().localNode().id();

        Integer nearKey = key(ignite0, NOT_PRIMARY_AND_BACKUP);

        cache0.put(nearKey, 1); // Put should create near entry on grid0.

        for (int i = 0; i < GRID_CNT; i++) {
            UUID[] expReaders = aff.isPrimary(grid(i).localNode(), nearKey) ? new UUID[]{id0} : new UUID[]{};

            checkEntry(grid(i), nearKey, 1, i == 0, expReaders);
        }

        cache0.localEvict(Collections.singleton(nearKey));

        for (int i = 0; i < GRID_CNT; i++) {
            UUID[] expReaders = aff.isPrimary(grid(i).localNode(), nearKey) ? new UUID[]{id0} : new UUID[]{};

            checkEntry(grid(i), nearKey, 1, false, expReaders);
        }

        IgniteCache<Integer, Integer> primaryCache = G.ignite(
            (String) aff.mapKeyToNode(nearKey).attribute(ATTR_GRID_NAME)).cache(null);

        atomicClockModeDelay(cache0);

        primaryCache.put(nearKey, 2); // This put should see that near entry evicted on grid0 and remove reader.

        for (int i = 0; i < GRID_CNT; i++)
            checkEntry(grid(i), nearKey, 2, false);

        assertEquals((Integer)2, cache0.get(nearKey)); // Get should again create near entry on grid0.

        for (int i = 0; i < GRID_CNT; i++) {
            UUID[] expReaders = aff.isPrimary(grid(i).localNode(), nearKey) ? new UUID[]{id0} : new UUID[]{};

            checkEntry(grid(i), nearKey, 2, i == 0, expReaders);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    private void checkReaderRemove() throws Exception {
        Ignite ignite0 = grid(0);

        IgniteCache<Integer, Integer> cache0 = ignite0.cache(null);

        Affinity<Integer> aff = ignite0.affinity(null);

        UUID id0 = ignite0.cluster().localNode().id();

        Integer nearKey = key(ignite0, NOT_PRIMARY_AND_BACKUP);

        cache0.put(nearKey, 1); // Put should create near entry on grid0.

        for (int i = 0; i < GRID_CNT; i++) {
            UUID[] expReaders = aff.isPrimary(grid(i).localNode(), nearKey) ? new UUID[]{id0} : new UUID[]{};

            checkEntry(grid(i), nearKey, 1, i == 0, expReaders);
        }

        cache0.remove(nearKey); // Remove from grid0, this should remove readers on primary node.

        for (int i = 0; i < GRID_CNT; i++)
            checkEntry(grid(i), nearKey, null, i == 0);

        Ignite primaryNode = G.ignite((String) aff.mapKeyToNode(nearKey).attribute(ATTR_GRID_NAME));

        IgniteCache<Integer, Integer> primaryCache = primaryNode.cache(null);

        atomicClockModeDelay(primaryCache);

        primaryCache.put(nearKey, 2); // Put from primary, check there are no readers.

        checkEntry(primaryNode, nearKey, 2, false);
    }

    /**
     * @param ignite Node.
     * @param key Key.
     * @param val Expected value.
     * @param expectNear If {@code true} then near cache entry is expected.
     * @param expReaders Expected readers.
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    private void checkEntry(Ignite ignite, Integer key, @Nullable Integer val, boolean expectNear, UUID... expReaders)
        throws Exception {
        GridCacheAdapter<Integer, Integer> near = ((IgniteKernal) ignite).internalCache();

        assertTrue(near.isNear());

        GridCacheEntryEx nearEntry = near.peekEx(key);

        boolean expectDht = near.affinity().isPrimaryOrBackup(ignite.cluster().localNode(), key);

        if (expectNear) {
            assertNotNull("No near entry for: " + key + ", grid: " + ignite.name(), nearEntry);

            assertEquals("Unexpected value for grid: " + ignite.name(),
                val,
                CU.value(nearEntry.info().value(), near.context(), false));

            assertEquals("Unexpected value for grid: " + ignite.name(),
                val,
                ignite.cache(near.name()).localPeek(key, CachePeekMode.ONHEAP));
        }
        else
            assertNull("Unexpected near entry: " + nearEntry + ", grid: " + ignite.name(), nearEntry);

        GridDhtCacheAdapter<Integer, Integer> dht = ((GridNearCacheAdapter<Integer, Integer>)near).dht();

        GridDhtCacheEntry dhtEntry = (GridDhtCacheEntry)dht.peekEx(key);

        if (expectDht) {
            assertNotNull("No dht entry for: " + key + ", grid: " + ignite.name(), dhtEntry);

            Collection<UUID> readers = dhtEntry.readers();

            assertEquals(expReaders.length, readers.size());

            for (UUID reader : expReaders)
                assertTrue(readers.contains(reader));

            assertEquals("Unexpected value for grid: " + ignite.name(),
                val,
                CU.value(dhtEntry.info().value(), dht.context(), false));

            assertEquals("Unexpected value for grid: " + ignite.name(),
                val,
                ignite.cache(near.name()).localPeek(key, CachePeekMode.ONHEAP));
        }
        else
            assertNull("Unexpected dht entry: " + dhtEntry + ", grid: " + ignite.name(), dhtEntry);

        if (!expectNear && !expectDht) {
            assertNull("Unexpected peek value for grid: " + ignite.name(), ignite.cache(near.name()).localPeek(key,
                CachePeekMode.ONHEAP));
        }
    }

    /**
     * @param ignite Grid.
     * @param mode One of {@link #PRIMARY}, {@link #BACKUP} or {@link #NOT_PRIMARY_AND_BACKUP}.
     * @return Key with properties specified by the given mode.
     */
    private Integer key(Ignite ignite, int mode) {
        Affinity<Integer> aff = ignite.affinity(null);

        Integer key = null;

        for (int i = lastKey + 1; i < 1_000_000; i++) {
            boolean pass = false;

            switch(mode) {
                case PRIMARY: pass = aff.isPrimary(ignite.cluster().localNode(), i); break;

                case BACKUP: pass = aff.isBackup(ignite.cluster().localNode(), i); break;

                case NOT_PRIMARY_AND_BACKUP: pass = !aff.isPrimaryOrBackup(ignite.cluster().localNode(), i); break;

                default: fail();
            }

            lastKey = i;

            if (pass) {
                key = i;

                break;
            }
        }

        assertNotNull(key);

        return key;
    }

    /**
     * @param backups Backups number.
     * @param writeOrderMode Write order mode.
     * @throws Exception If failed.
     */
    private void startGrids(int backups, CacheAtomicWriteOrderMode writeOrderMode) throws Exception {
        this.backups = backups;

        this.writeOrderMode = writeOrderMode;

        startGrids(GRID_CNT);

        awaitPartitionMapExchange();

        log.info("Grids: ");

        for (int i = 0; i < GRID_CNT; i++)
            log.info(grid(i).name() + ": " + grid(i).localNode().id());
    }

    /**
     *
     */
    private static class Processor implements EntryProcessor<Integer, Integer, Void>, Serializable {
        /** */
        private final Integer newVal;

        /**
         * @param newVal New value.
         */
        private Processor(Integer newVal) {
            this.newVal = newVal;
        }

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Integer, Integer> e, Object... args) {
            e.setValue(newVal);

            return null;
        }
    }
}