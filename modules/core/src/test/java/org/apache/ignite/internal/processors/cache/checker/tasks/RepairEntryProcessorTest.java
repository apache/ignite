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

package org.apache.ignite.internal.processors.cache.checker.tasks;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.cache.processor.MutableEntry;
import junit.framework.TestCase;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CACHE_REMOVED_ENTRIES_TTL;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Isolated unit test for {@link RepairEntryProcessor}.
 */
public class RepairEntryProcessorTest  extends TestCase {
    /**
     *
     */
    private static final String OLD_VALUE = "old_value";

    /** Old cache value. */
    private static final CacheObject OLD_CACHE_VALUE = new CacheObjectImpl(OLD_VALUE, OLD_VALUE.getBytes());

    /**
     *
     */
    private static final String NEW_VALUE = "new_value";

    /**
     * Value at the recheck phase. It uses to check parallel updates.
     */
    private static final String RECHECK_VALUE = "updated_value";

    /**
     *
     */
    private static final CacheObject RECHECK_CACHE_VALUE = new CacheObjectImpl(RECHECK_VALUE, RECHECK_VALUE.getBytes());

    /** Local node id. */
    private static final UUID LOCAL_NODE_ID = UUID.randomUUID();

    /** Other node id. */
    private static final UUID OTHRER_NODE_ID = UUID.randomUUID();

    /** Remove queue max size. */
    private static final int RMV_QUEUE_MAX_SIZE = 12;

    /**
     *
     */
    private GridCacheContext cctx;

    /**
     *
     */
    @Before
    public void setUp() throws Exception {
        cctx = mock(GridCacheContext.class);

        when(cctx.localNodeId()).thenReturn(LOCAL_NODE_ID);

        when(cctx.config()).thenReturn(new CacheConfiguration().setAtomicityMode(CacheAtomicityMode.ATOMIC));

        System.setProperty(IGNITE_CACHE_REMOVED_ENTRIES_TTL, "10000");
    }

    /**
     *
     */
    @After
    public void tearDown() {
        System.clearProperty(IGNITE_CACHE_REMOVED_ENTRIES_TTL);
    }

    /**
     * Force should apply remove.
     */
    @Test
    public void testForceRepairApplyRemove() {
        final boolean forceRepair = true;

        Map<UUID, VersionedValue> data = new HashMap<>();
        data.put(OTHRER_NODE_ID, new VersionedValue(
            OLD_CACHE_VALUE,
            new GridCacheVersion(1, 1, 1),
            1,
            1
        ));

        RepairEntryProcessor repairProcessor = new RepairEntryProcessorStub(
            null,
            data,
            RMV_QUEUE_MAX_SIZE,
            forceRepair,
            new AffinityTopologyVersion(1)
        ).setKeyVersion(new GridCacheVersion(0, 0, 0));

        MutableEntry entry = mock(MutableEntry.class);

        assertEquals(repairProcessor.process(entry), RepairEntryProcessor.RepairStatus.SUCCESS);

        verify(entry, times(1)).remove();
    }

    /**
     * Force should apply new value.
     */
    @Test
    public void testForceRepairApplyValue() {
        final boolean forceRepair = true;

        Map<UUID, VersionedValue> data = new HashMap<>();
        data.put(OTHRER_NODE_ID, new VersionedValue(
            OLD_CACHE_VALUE,
            new GridCacheVersion(1, 1, 1),
            1,
            1
        ));

        RepairEntryProcessor repairProcessor = new RepairEntryProcessorStub(
            NEW_VALUE,
            data,
            RMV_QUEUE_MAX_SIZE,
            forceRepair,
            new AffinityTopologyVersion(1)
        ).setKeyVersion(new GridCacheVersion(0, 0, 0));

        MutableEntry entry = mock(MutableEntry.class);

        assertEquals(repairProcessor.process(entry), RepairEntryProcessor.RepairStatus.SUCCESS);

        verify(entry, times(1)).setValue(NEW_VALUE);
    }

    /**
     * Setting value where old value not null.
     */
    @Test
    public void testSetValueWithoutParallelUpdateWhereCurrentRecheckNotNull() {
        Map<UUID, VersionedValue> data = new HashMap<>();
        data.put(LOCAL_NODE_ID, new VersionedValue(
            OLD_CACHE_VALUE,
            new GridCacheVersion(1, 1, 1),
            1,
            1
        ));

        RepairEntryProcessor repairProcessor = new RepairEntryProcessorStub(
            NEW_VALUE,
            data,
            RMV_QUEUE_MAX_SIZE,
            false,
            new AffinityTopologyVersion(1)
        ).setKeyVersion(new GridCacheVersion(1, 1, 1));

        MutableEntry entry = mock(MutableEntry.class);
        when(entry.getValue()).thenReturn(OLD_CACHE_VALUE);

        assertEquals(repairProcessor.process(entry), RepairEntryProcessor.RepairStatus.SUCCESS);

        verify(entry, times(1)).setValue(NEW_VALUE);
    }

    /**
     * Removing value where old value not null.
     */
    @Test
    public void testRemoveValueWithoutParallelUpdateWhereCurrentRecheckNotNull() {
        Map<UUID, VersionedValue> data = new HashMap<>();
        data.put(LOCAL_NODE_ID, new VersionedValue(
            OLD_CACHE_VALUE,
            new GridCacheVersion(1, 1, 1),
            1,
            1
        ));

        RepairEntryProcessor repairProcessor = new RepairEntryProcessorStub(
            null,
            data,
            RMV_QUEUE_MAX_SIZE,
            false,
            new AffinityTopologyVersion(1)
        ).setKeyVersion(new GridCacheVersion(1, 1, 1));

        MutableEntry entry = mock(MutableEntry.class);
        when(entry.getValue()).thenReturn(OLD_CACHE_VALUE);

        assertEquals(repairProcessor.process(entry), RepairEntryProcessor.RepairStatus.SUCCESS);

        verify(entry, times(1)).remove();
    }

    /**
     * Setting value under parallel update where old value not null, but changed.
     */
    @Test
    public void testWriteValueParallelUpdateWhereCurrentRecheckNotNull() {
        Map<UUID, VersionedValue> data = new HashMap<>();
        data.put(LOCAL_NODE_ID, new VersionedValue(
            OLD_CACHE_VALUE,
            new GridCacheVersion(1, 1, 1),
            1,
            1
        ));

        RepairEntryProcessor repairProcessor = new RepairEntryProcessorStub(
            NEW_VALUE,
            data,
            RMV_QUEUE_MAX_SIZE,
            false,
            new AffinityTopologyVersion(1)
        ).setKeyVersion(new GridCacheVersion(2, 2, 2));

        MutableEntry entry = mock(MutableEntry.class);
        when(entry.getValue()).thenReturn(OLD_CACHE_VALUE);

        assertEquals(repairProcessor.process(entry), RepairEntryProcessor.RepairStatus.CONCURRENT_MODIFICATION);
    }

    /**
     * It mean that a partition under load.
     */
    @Test
    public void testRecheckVersionNullCurrentValueExist() {
        RepairEntryProcessor repairProcessor = new RepairEntryProcessorStub(
            NEW_VALUE,
            new HashMap<>(),
            RMV_QUEUE_MAX_SIZE,
            false,
            new AffinityTopologyVersion(1)
        ).setKeyVersion(new GridCacheVersion(1, 1, 1));

        MutableEntry entry = mock(MutableEntry.class);

        assertEquals(repairProcessor.process(entry), RepairEntryProcessor.RepairStatus.CONCURRENT_MODIFICATION);
    }

    /**
     * Setting new value when old value null and TTL of entry isn't expired and update counter of partition in the
     * delete queue border.
     */
    @Test
    public void testRecheckVersionNullAndTtlEntryShouldNotAlreadyRemovedAndNewUpdateCounterLessDelQueueSizeOpRemove() {
        Map<UUID, VersionedValue> data = new HashMap<>();
        data.put(OTHRER_NODE_ID, new VersionedValue(
            OLD_CACHE_VALUE,
            new GridCacheVersion(1, 1, 1),
            1,
            System.currentTimeMillis()
        ));

        RepairEntryProcessor repairProcessor = new RepairEntryProcessorStub(
            null,
            data,
            RMV_QUEUE_MAX_SIZE,
            false,
            new AffinityTopologyVersion(1)
        ).setKeyVersion(new GridCacheVersion(0, 0, 0));

        MutableEntry entry = mock(MutableEntry.class);

        assertEquals(repairProcessor.process(entry), RepairEntryProcessor.RepairStatus.SUCCESS);

        verify(entry, times(1)).remove();
    }

    /**
     * Removing new value when old value null and TTL of entry isn't expired and update counter of partition in the
     * delete queue border.
     */
    @Test
    public void testRecheckVersionNullAndTtlEntryShouldNotAlreadyRemovedAndNewUpdateCounterLessDelQueueSizeOpSet() {
        Map<UUID, VersionedValue> data = new HashMap<>();
        data.put(OTHRER_NODE_ID, new VersionedValue(
            OLD_CACHE_VALUE,
            new GridCacheVersion(1, 1, 1),
            1,
            System.currentTimeMillis()
        ));

        RepairEntryProcessor repairProcessor = new RepairEntryProcessorStub(
            RECHECK_VALUE,
            data,
            RMV_QUEUE_MAX_SIZE,
            false,
            new AffinityTopologyVersion(1)
        ).setKeyVersion(new GridCacheVersion(0, 0, 0));

        MutableEntry entry = mock(MutableEntry.class);

        assertEquals(repairProcessor.process(entry), RepairEntryProcessor.RepairStatus.SUCCESS);

        verify(entry, times(1)).setValue(RECHECK_VALUE);
    }

    /**
     * If current value other then old, it detects {@link RepairEntryProcessor.RepairStatus.CONCURRENT_MODIFICATION}.
     */
    @Test
    public void testEntryWasChangedDuringRepairAtOtherValue() {
        Map<UUID, VersionedValue> data = new HashMap<>();
        data.put(LOCAL_NODE_ID, new VersionedValue(
            new CacheObjectImpl(OLD_VALUE, OLD_VALUE.getBytes()),
            new GridCacheVersion(1, 1, 1),
            1,
            1
        ));

        RepairEntryProcessor repairProcessor = new RepairEntryProcessorStub(
            null,
            data,
            RMV_QUEUE_MAX_SIZE,
            false,
            new AffinityTopologyVersion(1)
        ).setKeyVersion(new GridCacheVersion(0, 0, 0));

        MutableEntry entry = mock(MutableEntry.class);
        when(entry.getValue()).thenReturn(RECHECK_CACHE_VALUE);

        assertEquals(repairProcessor.process(entry), RepairEntryProcessor.RepairStatus.CONCURRENT_MODIFICATION);
    }

    /**
     * If current value other then old, it detects {@link RepairEntryProcessor.RepairStatus.CONCURRENT_MODIFICATION}.
     */
    @Test
    public void testEntryWasChangedDuringRepairAtNull() {
        Map<UUID, VersionedValue> data = new HashMap<>();
        data.put(LOCAL_NODE_ID, new VersionedValue(
            new CacheObjectImpl(OLD_VALUE, OLD_VALUE.getBytes()),
            new GridCacheVersion(1, 1, 1),
            1,
            1
        ));

        RepairEntryProcessor repairProcessor = new RepairEntryProcessorStub(
            null,
            data,
            RMV_QUEUE_MAX_SIZE,
            false,
            new AffinityTopologyVersion(1)
        ).setKeyVersion(new GridCacheVersion(0, 0, 0));

        MutableEntry entry = mock(MutableEntry.class);
        when(entry.getValue()).thenReturn(null);

        assertEquals(repairProcessor.process(entry), RepairEntryProcessor.RepairStatus.CONCURRENT_MODIFICATION);
    }

    /**
     * If current value other then old, it detects {@link RepairEntryProcessor.RepairStatus.CONCURRENT_MODIFICATION}.
     */
    @Test
    public void testEntryWasChangedDuringRepairFromNullToValue() {
        Map<UUID, VersionedValue> data = new HashMap<>();
        data.put(OTHRER_NODE_ID, new VersionedValue(
            OLD_CACHE_VALUE,
            new GridCacheVersion(1, 1, 1),
            1,
            1
        ));

        RepairEntryProcessor repairProcessor = new RepairEntryProcessorStub(
            null,
            data,
            RMV_QUEUE_MAX_SIZE,
            false,
            new AffinityTopologyVersion(1)
        ).setKeyVersion(new GridCacheVersion(1, 1, 1));

        MutableEntry entry = mock(MutableEntry.class);
        when(entry.getValue()).thenReturn(new CacheObjectImpl(RECHECK_VALUE, RECHECK_VALUE.getBytes()));

        assertEquals(repairProcessor.process(entry), RepairEntryProcessor.RepairStatus.CONCURRENT_MODIFICATION);
    }

    /**
     * If ttl expired for old null value, it can't solve ABA problem and should return {@link
     * RepairEntryProcessor.RepairStatus.FAIL}.
     */
    @Test
    public void testRecheckVersionNullAndTtlEntryExpired() {
        Map<UUID, VersionedValue> data = new HashMap<>();
        data.put(OTHRER_NODE_ID, new VersionedValue(
            new CacheObjectImpl(OLD_VALUE, OLD_VALUE.getBytes()),
            new GridCacheVersion(1, 1, 1),
            1,
            1
        ));

        RepairEntryProcessor repairProcessor = new RepairEntryProcessorStub(
            null,
            data,
            RMV_QUEUE_MAX_SIZE,
            false,
            new AffinityTopologyVersion(1)
        ).setKeyVersion(new GridCacheVersion(0, 0, 0));

        MutableEntry entry = mock(MutableEntry.class);

        assertEquals(repairProcessor.process(entry), RepairEntryProcessor.RepairStatus.FAIL);
    }

    /**
     * If Deleted queue expired for old null value, it can't solve ABA problem and should return {@link
     * RepairEntryProcessor.RepairStatus.FAIL}.
     */
    @Test
    public void testRecheckVersionNullAndDefDelQueueExpired() {
        Map<UUID, VersionedValue> data = new HashMap<>();
        data.put(OTHRER_NODE_ID, new VersionedValue(
            new CacheObjectImpl(OLD_VALUE, OLD_VALUE.getBytes()),
            new GridCacheVersion(1, 1, 1),
            1,
            1
        ));

        RepairEntryProcessor repairProcessor = new RepairEntryProcessorStub(
            null,
            data,
            RMV_QUEUE_MAX_SIZE,
            false,
            new AffinityTopologyVersion(1)
        )
            .setKeyVersion(new GridCacheVersion(0, 0, 0))
            .setUpdateCounter(100); // More than 32!

        MutableEntry entry = mock(MutableEntry.class);

        assertEquals(repairProcessor.process(entry), RepairEntryProcessor.RepairStatus.FAIL);
    }

    /**
     * Stub for testing approach, mocks methods.
     */
    private class RepairEntryProcessorStub extends RepairEntryProcessor {
        /**
         *
         */
        private GridCacheContext ctx = cctx;

        /**
         *
         */
        private boolean topChanged = false;

        /**
         *
         */
        private GridCacheVersion keyVer;

        /**
         *
         */
        private long updateCntr = 1;

        /**
         * @param val Value.
         * @param data Data.
         * @param rmvQueueMaxSize Remove queue max size.
         * @param forceRepair Force repair.
         * @param startTopVer Start topology version.
         */
        public RepairEntryProcessorStub(
            Object val,
            Map<UUID, VersionedValue> data,
            long rmvQueueMaxSize,
            boolean forceRepair,
            AffinityTopologyVersion startTopVer
        ) {
            super(val, data, rmvQueueMaxSize, forceRepair, startTopVer);
        }

        /**
         *
         */
        @Override protected GridCacheContext cacheContext(MutableEntry entry) {
            return ctx;
        }

        /**
         *
         */
        @Override protected boolean topologyChanged(GridCacheContext cctx, AffinityTopologyVersion expTop) {
            return topChanged;
        }

        /**
         *
         */
        @Override protected GridCacheVersion keyVersion(MutableEntry entry) {
            return keyVer;
        }

        /**
         *
         */
        @Override protected long updateCounter(GridCacheContext cctx, Object affKey) {
            return updateCntr;
        }

        /**
         *
         */
        public RepairEntryProcessorStub setContext(GridCacheContext ctx) {
            this.ctx = ctx;

            return this;
        }

        /**
         *
         */
        public RepairEntryProcessorStub setTopologyChanged(boolean topChanged) {
            this.topChanged = topChanged;

            return this;
        }

        /**
         *
         */
        public RepairEntryProcessorStub setKeyVersion(GridCacheVersion keyVer) {
            this.keyVer = keyVer;

            return this;
        }

        /**
         *
         */
        public RepairEntryProcessorStub setUpdateCounter(long updateCntr) {
            this.updateCntr = updateCntr;

            return this;
        }
    }
}