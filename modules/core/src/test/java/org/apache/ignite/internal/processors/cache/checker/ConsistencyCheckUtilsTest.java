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

package org.apache.ignite.internal.processors.cache.checker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import junit.framework.TestCase;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityManager;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.calculateValueToFixWith;
import static org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm.MAJORITY;
import static org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm.LATEST;
import static org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm.PRIMARY;
import static org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm.REMOVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for utility methods for the partition reconciliation.
 */
public class ConsistencyCheckUtilsTest extends TestCase {
    /** Node 2. */
    private static final UUID NODE_2 = UUID.randomUUID();

    /** Node 3. */
    private static final UUID NODE_3 = UUID.randomUUID();

    /** Node 4. */
    private static final UUID NODE_4 = UUID.randomUUID();

    /** Node 1. */
    private static final UUID NODE_1 = UUID.randomUUID();

    /** Test key. */
    private static final KeyCacheObject TEST_KEY = new KeyCacheObjectImpl(123, null, -1);

    /** Cctx. */
    private GridCacheContext cctx = mock(GridCacheContext.class);

    /** Topology. */
    private GridDhtPartitionTopology top = mock(GridDhtPartitionTopology.class);

    /** Aff. */
    private GridCacheAffinityManager aff = mock(GridCacheAffinityManager.class);

    /** Owners. */
    private List owners = mock(List.class);

    {
        when(cctx.topology()).thenReturn(top);

        when(top.owners(anyInt(), any())).thenReturn(owners);

        when(cctx.affinity()).thenReturn(aff);

        when(aff.partition(any())).thenReturn(1);
    }

    /**
     * Test different scenario of {@link ConsistencyCheckUtils#checkConsistency}
     */
    @Test
    public void testCheckConsistency() {
        Map<UUID, GridCacheVersion> oldKey = new HashMap<>();
        oldKey.put(NODE_1, version(1));
        oldKey.put(NODE_2, version(3));
        oldKey.put(NODE_3, version(3));
        oldKey.put(NODE_4, version(2));

        {
            Map<UUID, VersionedValue> actualKey = new HashMap<>(); // All keys was removed

            assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey, 4));
        }

        {
            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(NODE_1, versionedValue(1));
            actualKey.put(NODE_2, versionedValue(3));
            actualKey.put(NODE_3, versionedValue(4)); // Max version increase
            actualKey.put(NODE_4, versionedValue(2));

            assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey, 4));
        }

        {
            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(NODE_1, versionedValue(1));
            actualKey.put(NODE_2, versionedValue(3)); // Max of node 3 was removed
            actualKey.put(NODE_4, versionedValue(2));

            assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey, 4));
        }

        {
            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(NODE_1, versionedValue(3)); // Min value like max
            actualKey.put(NODE_2, versionedValue(3));
            actualKey.put(NODE_3, versionedValue(3));
            actualKey.put(NODE_4, versionedValue(3));

            assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey, 4));
        }

        {
            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(NODE_1, versionedValue(4)); // Min value greater then max
            actualKey.put(NODE_2, versionedValue(3));
            actualKey.put(NODE_3, versionedValue(3));
            actualKey.put(NODE_4, versionedValue(4));

            assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey, 4));
        }

        {
            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(NODE_1, versionedValue(1)); // Nothing changed.
            actualKey.put(NODE_2, versionedValue(3));
            actualKey.put(NODE_3, versionedValue(3));
            actualKey.put(NODE_4, versionedValue(2));

            assertFalse(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey, 4));
        }

        {
            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(NODE_1, versionedValue(2)); // Not all min values were incremented.
            actualKey.put(NODE_2, versionedValue(3));
            actualKey.put(NODE_3, versionedValue(3));
            actualKey.put(NODE_4, versionedValue(3));

            assertFalse(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey, 4));
        }

        {
            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(NODE_2, versionedValue(3)); // Remove of one value is not enough
            actualKey.put(NODE_3, versionedValue(3));
            actualKey.put(NODE_4, versionedValue(3));

            assertFalse(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey, 4));
        }
    }

    /**
     * Test different scenario of {@link ConsistencyCheckUtils#checkConsistency}. When one of max group changed.
     */
    @Test
    public void testCheckConsistencyMaxGroup() {
        {
            Map<UUID, GridCacheVersion> oldKey = new HashMap<>();
            oldKey.put(NODE_1, version(1));
            oldKey.put(NODE_2, version(1));

            {
                Map<UUID, VersionedValue> actualKey = new HashMap<>();
                actualKey.put(NODE_1, versionedValue(1));

                assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey, 3));
            }

            {
                Map<UUID, VersionedValue> actualKey = new HashMap<>();
                actualKey.put(NODE_2, versionedValue(1));

                assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey, 3));
            }
        }

        {
            Map<UUID, GridCacheVersion> oldKey = new HashMap<>();
            oldKey.put(NODE_1, version(1));
            oldKey.put(NODE_2, version(2));
            oldKey.put(NODE_3, version(3));

            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(NODE_3, versionedValue(4));

            assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey, 3));
        }

        {
            Map<UUID, GridCacheVersion> oldKey = new HashMap<>();
            oldKey.put(NODE_1, version(3));
            oldKey.put(NODE_2, version(2));
            oldKey.put(NODE_3, version(1));

            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(NODE_1, versionedValue(4));

            assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey, 3));
        }
    }

    /**
     * If a max element missing in set of old keys, we must swap old to actual keys.
     */
    @Test
    public void testOldKeySizeLessThenOwnerAndMaxElementIsMissing() {
        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> oldKeys = new HashMap<>();
        Map<UUID, GridCacheVersion> oldKeyVers = new HashMap<>();
        oldKeyVers.put(NODE_1, version(3));
        oldKeyVers.put(NODE_2, version(2));
        oldKeyVers.put(NODE_3, version(1));
        oldKeys.put(TEST_KEY, oldKeyVers);

        Map<KeyCacheObject, Map<UUID, VersionedValue>> actualKeys = new HashMap<>();
        Map<UUID, VersionedValue> actualKeyVers = new HashMap<>();
        actualKeyVers.put(NODE_4, versionedValue(4));
        actualKeyVers.put(NODE_1, versionedValue(3));
        actualKeyVers.put(NODE_2, versionedValue(2));
        actualKeyVers.put(NODE_3, versionedValue(1));
        actualKeys.put(TEST_KEY, actualKeyVers);

        when(owners.size()).thenReturn(4);

        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> recheckNeeded
            = ConsistencyCheckUtils.checkConflicts(oldKeys, actualKeys, cctx, null);

        assertEquals(1, recheckNeeded.size());
        assertEquals(recheckNeeded.get(TEST_KEY).size(), 4);
        assertEquals(recheckNeeded.get(TEST_KEY).get(NODE_4), version(4));
    }

    /**
     * Old key count less then owners, and one of old key was removed from actual key set.
     */
    @Test
    public void testOldKeySizeLessThenOwnerAndAnyOldKeyIsMissingInActualKeysCheckSuccess() {
        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> oldKeys = new HashMap<>();
        Map<UUID, GridCacheVersion> oldKeyVers = new HashMap<>();
        oldKeyVers.put(NODE_1, version(3));
        oldKeyVers.put(NODE_2, version(2));
        oldKeyVers.put(NODE_3, version(1));
        oldKeys.put(TEST_KEY, oldKeyVers);

        Map<KeyCacheObject, Map<UUID, VersionedValue>> actualKeys = new HashMap<>();
        Map<UUID, VersionedValue> actualKeyVers = new HashMap<>();
        actualKeyVers.put(NODE_4, versionedValue(4));
        actualKeyVers.put(NODE_1, versionedValue(3));
        actualKeyVers.put(NODE_2, versionedValue(2));
        actualKeys.put(TEST_KEY, actualKeyVers);

        when(owners.size()).thenReturn(4);

        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> recheckNeeded
            = ConsistencyCheckUtils.checkConflicts(oldKeys, actualKeys, cctx, null);

        assertEquals(0, recheckNeeded.size());
    }

    /**
     * Old key count less then owners, and actual key is greater then old max.
     */
    @Test
    public void testOldKeySizeLessThenOwnerAndAnyActualKeyIsGreaterThenOldMax() {
        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> oldKeys = new HashMap<>();
        Map<UUID, GridCacheVersion> oldKeyVers = new HashMap<>();
        oldKeyVers.put(NODE_1, version(3));
        oldKeyVers.put(NODE_2, version(2));
        oldKeyVers.put(NODE_3, version(1));
        oldKeys.put(TEST_KEY, oldKeyVers);

        Map<KeyCacheObject, Map<UUID, VersionedValue>> actualKeys = new HashMap<>();
        Map<UUID, VersionedValue> actualKeyVers = new HashMap<>();
        actualKeyVers.put(NODE_4, versionedValue(2));
        actualKeyVers.put(NODE_1, versionedValue(3));
        actualKeyVers.put(NODE_2, versionedValue(5));
        actualKeys.put(TEST_KEY, actualKeyVers);

        when(owners.size()).thenReturn(4);

        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> recheckNeeded
            = ConsistencyCheckUtils.checkConflicts(oldKeys, actualKeys, cctx, null);

        assertEquals(0, recheckNeeded.size());
    }

    /**
     * Return value from primary node when {@link RepairAlgorithm.PRIMARY} selected and value exist.
     */
    @Test
    public void testCalcValuePrimaryNodeHasValue() throws IgniteCheckedException {
        Map<UUID, VersionedValue> nodeToVersionedValues = new HashMap<>();
        VersionedValue verVal = versionedValue(1, new CacheObjectImpl(1, null));
        nodeToVersionedValues.put(NODE_1, verVal);

        CacheObject val = calculateValueToFixWith(PRIMARY, nodeToVersionedValues, NODE_1, null, 4);

        assertEquals(verVal.value().valueBytes(cacheObjectContext()), val.valueBytes(cacheObjectContext()));
    }

    /**
     * Return null from primary node when {@link RepairAlgorithm.PRIMARY} selected and value doesn't exist.
     */
    @Test
    public void testCalcValuePrimaryNodeDoesNotHaveValue() throws IgniteCheckedException {
        CacheObject val = calculateValueToFixWith(PRIMARY, new HashMap<>(), NODE_1, null, 4);

        assertNull(val);
    }

    /**
     * Return null when {@link RepairAlgorithm.LATEST} selected and value doesn't exist.
     */
    @Test
    public void testCalcValueMaxGridVersionNodeDoesNotHaveValue() throws IgniteCheckedException {
        CacheObject val = calculateValueToFixWith(LATEST, new HashMap<>(), NODE_1, null, 4);

        assertNull(val);
    }

    /**
     * Return value with max {@link GridCacheVersion} when {@link RepairAlgorithm.LATEST} selected and value exist.
     */
    @Test
    public void testCalcValueMaxGridVersionNodeFindMaxVersion() throws IgniteCheckedException {
        Map<UUID, VersionedValue> nodeToVersionedValues = new HashMap<>();
        VersionedValue ver1 = versionedValue(1, new CacheObjectImpl(644, "644".getBytes()));
        VersionedValue ver2 = versionedValue(2, new CacheObjectImpl(331, "331".getBytes()));
        VersionedValue maxVerVal = versionedValue(5, new CacheObjectImpl(232, "232".getBytes()));
        VersionedValue ver4 = versionedValue(3, new CacheObjectImpl(165, "165".getBytes()));
        nodeToVersionedValues.put(NODE_1, ver1);
        nodeToVersionedValues.put(NODE_2, ver2);
        nodeToVersionedValues.put(NODE_3, maxVerVal);
        nodeToVersionedValues.put(NODE_4, ver4);

        CacheObject val = calculateValueToFixWith(LATEST, nodeToVersionedValues, NODE_1, null, 4);

        assertEquals(maxVerVal.value().valueBytes(cacheObjectContext()), val.valueBytes(cacheObjectContext()));
    }

    /**
     * Return null when {@link RepairAlgorithm.MAJORITY} selected and values doesn't exist.
     */
    @Test
    public void testCalcValueMajorityNodeDoesNotHaveValue() throws IgniteCheckedException {
        CacheObject val = calculateValueToFixWith(MAJORITY, new HashMap<>(), NODE_1, null, 4);

        assertNull(val);
    }

    /**
     * Return null when {@link RepairAlgorithm.MAJORITY} selected and quorum doesn't exist.
     */
    @Test
    public void testCalcValueMajorityMajorityWithoutQuorum() throws IgniteCheckedException {
        Map<UUID, VersionedValue> nodeToVersionedValues = new HashMap<>();
        VersionedValue ver1 = versionedValue(1, new CacheObjectImpl(644, null));
        nodeToVersionedValues.put(NODE_1, ver1);

        CacheObject val = calculateValueToFixWith(MAJORITY, nodeToVersionedValues, NODE_1, null, 4);

        assertNull(val);
    }

    /**
     * Return majority (null) value when {@link RepairAlgorithm.MAJORITY} selected and value exist.
     */
    @Test
    public void testCalcValueMajorityNullValuesMoreThenValued() throws IgniteCheckedException {
        Map<UUID, VersionedValue> nodeToVersionedValues = new HashMap<>();
        VersionedValue ver1 = versionedValue(1, new CacheObjectImpl(644, "644".getBytes()));
        VersionedValue ver2 = versionedValue(2, new CacheObjectImpl(331, "331".getBytes()));
        VersionedValue ver3 = versionedValue(3, new CacheObjectImpl(232, "232".getBytes()));
        VersionedValue ver4 = versionedValue(4, new CacheObjectImpl(165, "165".getBytes()));

        // Full quorum
        nodeToVersionedValues.put(NODE_1, ver1);
        nodeToVersionedValues.put(NODE_2, ver2);
        nodeToVersionedValues.put(NODE_3, ver3);
        nodeToVersionedValues.put(NODE_4, ver4);
        nodeToVersionedValues.put(UUID.randomUUID(), ver4);
        // 3 nodes with null

        CacheObject val = calculateValueToFixWith(MAJORITY, nodeToVersionedValues, NODE_1, cacheObjectContext(), 8);

        assertNull(val);
    }

    /**
     * Return majority value when {@link RepairAlgorithm.MAJORITY} selected and value exist.
     */
    @Test
    public void testCalcValueMajorityByValue() throws IgniteCheckedException {
        Map<UUID, VersionedValue> nodeToVersionedValues = new HashMap<>();
        VersionedValue ver1 = versionedValue(1, new CacheObjectImpl(644, "644".getBytes()));
        VersionedValue ver2 = versionedValue(2, new CacheObjectImpl(331, "331".getBytes()));
        VersionedValue ver3 = versionedValue(3, new CacheObjectImpl(232, "232".getBytes()));
        VersionedValue ver4 = versionedValue(4, new CacheObjectImpl(165, "165".getBytes()));

        nodeToVersionedValues.put(NODE_1, ver1);
        nodeToVersionedValues.put(NODE_2, ver2);
        nodeToVersionedValues.put(NODE_3, ver3);
        nodeToVersionedValues.put(NODE_4, ver4);
        nodeToVersionedValues.put(UUID.randomUUID(), ver4);
        nodeToVersionedValues.put(UUID.randomUUID(), ver4);
        // 2 nodes with null

        CacheObject val = calculateValueToFixWith(MAJORITY, nodeToVersionedValues, NODE_1, cacheObjectContext(), 8);

        assertEquals(ver4.value().valueBytes(cacheObjectContext()), val.valueBytes(cacheObjectContext()));
    }

    /**
     * For all input must return null.
     */
    @Test
    public void testCalcValueRemoveByValue() throws IgniteCheckedException {
        assertNull(calculateValueToFixWith(REMOVE, new HashMap<>(), UUID.randomUUID(), null, 8));
    }

    /**
     *
     */
    private CacheObjectContext cacheObjectContext() throws IgniteCheckedException {
        CacheObjectContext coc = mock(CacheObjectContext.class);
        IgniteCacheObjectProcessor cop = mock(IgniteCacheObjectProcessor.class);
        GridKernalContext kernalCtx = mock(GridKernalContext.class);
        when(coc.kernalContext()).thenReturn(kernalCtx);
        when(kernalCtx.cacheObjects()).thenReturn(cop);
        when(cop.marshal(any(), any())).thenAnswer(ans -> ans.getArguments()[1].toString().getBytes());

        return coc;
    }

    /**
     *
     */
    private GridCacheVersion version(int ver) {
        return new GridCacheVersion(1, 0, ver);
    }

    /**
     *
     */
    private VersionedValue versionedValue(int ver, CacheObject val) {
        return new VersionedValue(val, new GridCacheVersion(1, 0, ver), 1, 1);
    }

    /**
     *
     */
    private VersionedValue versionedValue(int ver) {
        return versionedValue(ver, null);
    }
}
