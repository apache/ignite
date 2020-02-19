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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.cache.processor.EntryProcessor;
import junit.framework.TestCase;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.checker.objects.ExecutionResult;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionKeyVersion;
import org.apache.ignite.internal.processors.cache.checker.objects.RepairResult;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.processors.cache.verify.RepairMeta;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ConsoleTestLogger;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm.LATEST;
import static org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm.MAJORITY;
import static org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm.PRIMARY;
import static org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm.REMOVE;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link RepairRequestTask} with different inputs and repair algorithms.
 */
public class RepairRequestTaskTest extends TestCase {
    /**
     *
     */
    private static final String DEFAULT_CACHE_NAME = "default";

    /** Node 2. */
    private static final UUID NODE_2 = UUID.randomUUID();

    /** Node 3. */
    private static final UUID NODE_3 = UUID.randomUUID();

    /** Node 4. */
    private static final UUID NODE_4 = UUID.randomUUID();

    /** Node 5. */
    private static final UUID NODE_5 = UUID.randomUUID();

    /** Node 1. */
    private static final UUID NODE_1 = UUID.randomUUID();

    /** Key. */
    private static final String KEY = "some_key";

    /** Repair algorithm. */
    public RepairAlgorithm repairAlgorithm;

    /** Repair algorithm. */
    public boolean fixed;

    /**
     *
     */
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        RepairAlgorithm[] repairAlgorithms = {LATEST, PRIMARY, MAJORITY, REMOVE};

        for (RepairAlgorithm algorithm : repairAlgorithms) {
            params.add(new Object[] {algorithm, true});
            params.add(new Object[] {algorithm, false});
        }

        return params;
    }

    /**
     * Test can't resolve conflict and should use user algorithm.
     */
    @Test
    public void testNotFullSetOfOldKeysUsesUserRepairAlg() throws IllegalAccessException {
        for (Object[] parameter : parameters()) {
            repairAlgorithm = (RepairAlgorithm)parameter[0];
            fixed = (boolean)parameter[1];

            Map<PartitionKeyVersion, Map<UUID, VersionedValue>> data = new HashMap<>();
            PartitionKeyVersion key = new PartitionKeyVersion(null, new KeyCacheObjectImpl(), null);
            Map<UUID, VersionedValue> keyVers = new HashMap<>();
            keyVers.put(NODE_1, versionedValue("1", 1));
            keyVers.put(NODE_2, versionedValue("2", 2));
            keyVers.put(NODE_3, versionedValue("2", 2));
            keyVers.put(NODE_4, versionedValue("4", 4));
            data.put(key, keyVers);

            IgniteEx igniteMock = igniteMock(true);

            ExecutionResult<RepairResult> res = injectIgnite(repairJob(data, 5, 1), igniteMock).execute();

            assertTrue(res.getResult().keysToRepair().isEmpty());
            assertEquals(1, res.getResult().repairedKeys().size());

            Map.Entry<PartitionKeyVersion, RepairMeta> entry = res.getResult()
                .repairedKeys().entrySet().iterator().next();
            assertEquals(keyVers, entry.getValue().getPreviousValue());

            RepairMeta repairMeta = entry.getValue();
            assertTrue(repairMeta.fixed());
            assertEquals(repairAlgorithm, repairMeta.repairAlg());

            switch (repairAlgorithm) {
                case LATEST:
                    assertCacheObjectEquals(keyVers.get(NODE_4).value(), repairMeta.value());
                    break;
                case PRIMARY:
                    assertCacheObjectEquals(keyVers.get(NODE_1).value(), repairMeta.value());
                    break;
                case MAJORITY:
                    assertCacheObjectEquals(keyVers.get(NODE_2).value(), repairMeta.value());
                    break;
                case REMOVE:
                    assertCacheObjectEquals(null, repairMeta.value());
                    break;
            }
        }
    }

    /**
     * This reparation works with GRID_MAX_VERSION and ignores th user algorithm.
     */
    @Test
    public void testFullOwnerSetNotMaxAttempt() throws IllegalAccessException {
        for (Object[] parameter : parameters()) {
            repairAlgorithm = (RepairAlgorithm)parameter[0];
            fixed = (boolean)parameter[1];

            Map<PartitionKeyVersion, Map<UUID, VersionedValue>> data = new HashMap<>();
            PartitionKeyVersion key = new PartitionKeyVersion(null, new KeyCacheObjectImpl(), null);
            Map<UUID, VersionedValue> keyVers = new HashMap<>();
            keyVers.put(NODE_1, versionedValue("1", 1));
            keyVers.put(NODE_2, versionedValue("2", 2));
            data.put(key, keyVers);

            IgniteEx igniteMock = igniteMock(fixed);

            ExecutionResult<RepairResult> res = injectIgnite(repairJob(data, 2, 1), igniteMock).execute();

            if (fixed) {
                assertTrue(res.getResult().keysToRepair().isEmpty());
                assertEquals(1, res.getResult().repairedKeys().size());

                Map.Entry<PartitionKeyVersion, RepairMeta> entry = res.getResult()
                    .repairedKeys().entrySet().iterator().next();
                assertEquals(keyVers, entry.getValue().getPreviousValue());

                RepairMeta repairMeta = entry.getValue();
                assertTrue(repairMeta.fixed());
                assertEquals(LATEST, repairMeta.repairAlg());
            }
            else {
                assertTrue(res.getResult().repairedKeys().isEmpty());
                assertEquals(1, res.getResult().keysToRepair().size());

                Map.Entry<PartitionKeyVersion, Map<UUID, VersionedValue>> entry = res.getResult()
                    .keysToRepair().entrySet().iterator().next();

                assertEquals(keyVers, entry.getValue());
            }
        }
    }

    /**
     * Repair reached max attempts, it should use user algorithm.
     */
    @Test
    public void testFullOwnerSetMaxAttempt() throws IllegalAccessException {
        for (Object[] parameter : parameters()) {
            repairAlgorithm = (RepairAlgorithm)parameter[0];
            fixed = (boolean)parameter[1];
            Map<PartitionKeyVersion, Map<UUID, VersionedValue>> data = new HashMap<>();
            PartitionKeyVersion key = new PartitionKeyVersion(null, new KeyCacheObjectImpl(), null);
            Map<UUID, VersionedValue> keyVers = new HashMap<>();
            keyVers.put(NODE_1, versionedValue("1", 1));
            keyVers.put(NODE_2, versionedValue("2", 2));
            keyVers.put(NODE_3, versionedValue("2", 2));
            keyVers.put(NODE_4, versionedValue("4", 4));
            data.put(key, keyVers);

            IgniteEx igniteMock = igniteMock(true);

            final int lastAttempt = 3;
            ExecutionResult<RepairResult> res = injectIgnite(repairJob(data, 4, lastAttempt), igniteMock).execute();

            assertTrue(res.getResult().keysToRepair().isEmpty());
            assertEquals(1, res.getResult().repairedKeys().size());

            Map.Entry<PartitionKeyVersion, RepairMeta> entry = res.getResult()
                .repairedKeys().entrySet().iterator().next();
            assertEquals(keyVers, entry.getValue().getPreviousValue());

            RepairMeta repairMeta = entry.getValue();
            assertTrue(repairMeta.fixed());
            assertEquals(repairAlgorithm, repairMeta.repairAlg());

            switch (repairAlgorithm) {
                case LATEST:
                    assertCacheObjectEquals(keyVers.get(NODE_4).value(), repairMeta.value());
                    break;
                case PRIMARY:
                    assertCacheObjectEquals(keyVers.get(NODE_1).value(), repairMeta.value());
                    break;
                case MAJORITY:
                    assertCacheObjectEquals(keyVers.get(NODE_2).value(), repairMeta.value());
                    break;
                case REMOVE:
                    assertCacheObjectEquals(null, repairMeta.value());
                    break;
            }
        }
    }

    /**
     *
     */
    private void assertCacheObjectEquals(CacheObject exp, CacheObject actual) {
        assertEquals(value(exp), value(actual));
    }

    /**
     *
     */
    private String value(CacheObject cacheObj) {
        return cacheObj != null ? U.field(cacheObj, "val") : null;
    }

    /**
     *
     */
    private RepairRequestTask.RepairJob repairJob(
        Map<PartitionKeyVersion, Map<UUID, VersionedValue>> data,
        int owners,
        int repairAttempt
    ) {
        return new RepairRequestTask.RepairJob(
            data,
            DEFAULT_CACHE_NAME,
            repairAlgorithm,
            repairAttempt,
            new AffinityTopologyVersion(),
            1
        ) {
            @Override protected UUID primaryNodeId(GridCacheContext ctx, Object key) {
                return NODE_1;
            }

            @Override protected int owners(GridCacheContext ctx) {
                return owners;
            }

            @Override
            protected Object keyValue(GridCacheContext ctx, KeyCacheObject key) throws IgniteCheckedException {
                return KEY;
            }
        };
    }

    /**
     * @param val Value.
     * @param ver Version.
     */
    private VersionedValue versionedValue(String val, int ver) {
        return new VersionedValue(
            new CacheObjectImpl(val, val.getBytes()),
            new GridCacheVersion(ver, 1, ver),
            1,
            1
        );
    }

    /**
     *
     */
    private IgniteEx igniteMock(boolean invokeReturnFixed) {
        IgniteEx igniteMock = mock(IgniteEx.class);

        IgniteCache cacheMock = mock(IgniteCache.class);
        when(igniteMock.cache(DEFAULT_CACHE_NAME)).thenReturn(cacheMock);

        GridCacheContext ccMock = mock(GridCacheContext.class);
        IgniteInternalCache internalCacheMock = mock(IgniteInternalCache.class);
        when(igniteMock.cachex(DEFAULT_CACHE_NAME)).thenReturn(internalCacheMock);
        when(internalCacheMock.context()).thenReturn(ccMock);

        when(cacheMock.withKeepBinary()).thenReturn(cacheMock);
        when(cacheMock.invoke(any(), any(EntryProcessor.class))).thenReturn(invokeReturnFixed ?
            RepairEntryProcessor.RepairStatus.SUCCESS : RepairEntryProcessor.RepairStatus.FAIL);

        return igniteMock;
    }

    /**
     *
     */
    private RepairRequestTask.RepairJob injectIgnite(RepairRequestTask.RepairJob job,
        IgniteEx ignite) throws IllegalAccessException {
        Field igniteField = U.findField(RepairRequestTask.RepairJob.class, "ignite");
        igniteField.set(job, ignite);

        Field logField = U.findField(RepairRequestTask.RepairJob.class, "log");
        logField.set(job, new ConsoleTestLogger(this.getClass().getName()));

        return job;
    }
}
