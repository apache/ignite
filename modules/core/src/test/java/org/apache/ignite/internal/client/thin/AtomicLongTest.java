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

package org.apache.ignite.internal.client.thin;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.client.ClientAtomicConfiguration;
import org.apache.ignite.client.ClientAtomicLong;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.junit.Test;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Tests client atomic long.
 * Partition awareness tests are in {@link ThinClientPartitionAwarenessStableTopologyTest#testAtomicLong()}.
 */
public class AtomicLongTest extends AbstractThinClientTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected ClientConfiguration getClientConfiguration() {
        return super.getClientConfiguration().setPartitionAwarenessEnabled(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * Tests initial value setting.
     */
    @Test
    public void testCreateSetsInitialValue() {
        String name = "testCreateSetsInitialValue";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicLong atomicLong = client.atomicLong(name, 42, true);

            ClientAtomicLong atomicLongWithGrp = client.atomicLong(
                    name, new ClientAtomicConfiguration().setGroupName("grp"), 43, true);

            assertEquals(42, atomicLong.get());
            assertEquals(43, atomicLongWithGrp.get());
        }
    }

    /**
     * Tests that initial value is ignored when atomic long already exists.
     */
    @Test
    public void testCreateIgnoresInitialValueWhenAlreadyExists() {
        String name = "testCreateIgnoresInitialValueWhenAlreadyExists";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicLong atomicLong = client.atomicLong(name, 42, true);
            ClientAtomicLong atomicLong2 = client.atomicLong(name, -42, true);

            assertEquals(42, atomicLong.get());
            assertEquals(42, atomicLong2.get());
        }
    }

    /**
     * Tests that exception is thrown when atomic long does not exist.
     */
    @Test
    public void testOperationsThrowExceptionWhenAtomicLongDoesNotExist() {
        try (IgniteClient client = startClient(0)) {
            String name = "testOperationsThrowExceptionWhenAtomicLongDoesNotExist";
            ClientAtomicLong atomicLong = client.atomicLong(name, 0, true);
            atomicLong.close();

            assertDoesNotExistError(name, atomicLong::get);

            assertDoesNotExistError(name, atomicLong::incrementAndGet);
            assertDoesNotExistError(name, atomicLong::getAndIncrement);
            assertDoesNotExistError(name, atomicLong::decrementAndGet);
            assertDoesNotExistError(name, atomicLong::getAndDecrement);

            assertDoesNotExistError(name, () -> atomicLong.addAndGet(1));
            assertDoesNotExistError(name, () -> atomicLong.getAndAdd(1));

            assertDoesNotExistError(name, () -> atomicLong.getAndSet(1));
            assertDoesNotExistError(name, () -> atomicLong.compareAndSet(1, 2));
        }
    }

    /**
     * Tests removed property.
     */
    @Test
    public void testRemoved() {
        String name = "testRemoved";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicLong atomicLong = client.atomicLong(name, 0, false);
            assertNull(atomicLong);

            atomicLong = client.atomicLong(name, 1, true);
            assertFalse(atomicLong.removed());
            assertEquals(1, atomicLong.get());

            atomicLong.close();
            assertTrue(atomicLong.removed());
        }
    }

    /**
     * Tests increment, decrement, add.
     */
    @Test
    public void testIncrementDecrementAdd() {
        String name = "testIncrementDecrementAdd";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicLong atomicLong = client.atomicLong(name, 1, true);

            assertEquals(2, atomicLong.incrementAndGet());
            assertEquals(2, atomicLong.getAndIncrement());

            assertEquals(3, atomicLong.get());

            assertEquals(2, atomicLong.decrementAndGet());
            assertEquals(2, atomicLong.getAndDecrement());

            assertEquals(1, atomicLong.get());

            assertEquals(101, atomicLong.addAndGet(100));
            assertEquals(101, atomicLong.getAndAdd(-50));

            assertEquals(51, atomicLong.get());
        }
    }

    /**
     * Tests getAndSet.
     */
    @Test
    public void testGetAndSet() {
        String name = "testGetAndSet";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicLong atomicLong = client.atomicLong(name, 1, true);

            assertEquals(1, atomicLong.getAndSet(100));
            assertEquals(100, atomicLong.get());
        }
    }

    /**
     * Tests compareAndSet.
     */
    @Test
    public void testCompareAndSet() {
        String name = "testCompareAndSet";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicLong atomicLong = client.atomicLong(name, 1, true);

            assertFalse(atomicLong.compareAndSet(2, 3));
            assertEquals(1, atomicLong.get());

            assertTrue(atomicLong.compareAndSet(1, 4));
            assertEquals(4, atomicLong.get());
        }
    }

    /**
     * Tests atomic long with custom configuration.
     */
    @Test
    public void testCustomConfigurationPropagatesToServer() {
        ClientAtomicConfiguration cfg1 = new ClientAtomicConfiguration()
                .setAtomicSequenceReserveSize(64)
                .setBackups(2)
                .setCacheMode(CacheMode.PARTITIONED)
                .setGroupName("atomic-long-group-partitioned");

        ClientAtomicConfiguration cfg2 = new ClientAtomicConfiguration()
                .setAtomicSequenceReserveSize(32)
                .setBackups(3)
                .setCacheMode(CacheMode.REPLICATED)
                .setGroupName("atomic-long-group-replicated");

        String name = "testCustomConfiguration";

        try (IgniteClient client = startClient(0)) {
            client.atomicLong(name, cfg1, 1, true);
            client.atomicLong(name, cfg2, 2, true);
            client.atomicLong(name, 3, true);
        }

        List<IgniteInternalCache<?, ?>> caches = new ArrayList<>(grid(0).cachesx());
        assertEquals(4, caches.size());

        IgniteInternalCache<?, ?> partitionedCache = caches.get(1);
        IgniteInternalCache<?, ?> replicatedCache = caches.get(2);
        IgniteInternalCache<?, ?> dfltCache = caches.get(3);

        assertEquals("ignite-sys-atomic-cache@atomic-long-group-partitioned", partitionedCache.name());
        assertEquals("ignite-sys-atomic-cache@atomic-long-group-replicated", replicatedCache.name());
        assertEquals("ignite-sys-atomic-cache@default-ds-group", dfltCache.name());

        assertEquals(2, partitionedCache.configuration().getBackups());
        assertEquals(Integer.MAX_VALUE, replicatedCache.configuration().getBackups());
        assertEquals(1, dfltCache.configuration().getBackups());
    }

    /**
     * Tests atomic long with same name and group name, but different cache modes.
     */
    @Test
    public void testSameNameDifferentOptionsDoesNotCreateSecondAtomic() {
        String grpName = "testSameNameDifferentOptions";

        ClientAtomicConfiguration cfg1 = new ClientAtomicConfiguration()
                .setCacheMode(CacheMode.REPLICATED)
                .setGroupName(grpName);

        ClientAtomicConfiguration cfg2 = new ClientAtomicConfiguration()
                .setCacheMode(CacheMode.PARTITIONED)
                .setGroupName(grpName);

        String name = "testSameNameDifferentOptionsDoesNotCreateSecondAtomic";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicLong al1 = client.atomicLong(name, cfg1, 1, true);
            ClientAtomicLong al2 = client.atomicLong(name, cfg2, 2, true);
            ClientAtomicLong al3 = client.atomicLong(name, 3, true);

            assertEquals(1, al1.get());
            assertEquals(1, al2.get());
            assertEquals(3, al3.get());
        }

        List<IgniteInternalCache<?, ?>> caches = grid(0).cachesx().stream()
                .filter(c -> c.name().contains(grpName))
                .collect(Collectors.toList());

        assertEquals(1, caches.size());

        IgniteInternalCache<?, ?> replicatedCache = caches.get(0);

        assertEquals("ignite-sys-atomic-cache@testSameNameDifferentOptions", replicatedCache.name());
        assertEquals(Integer.MAX_VALUE, replicatedCache.configuration().getBackups());
    }

    /**
     * Asserts that "does not exist" error is thrown.
     *
     * @param name Atomic long name.
     * @param callable Callable.
     */
    private void assertDoesNotExistError(String name, Callable<Object> callable) {
        ClientException ex = (ClientException)assertThrows(null, callable, ClientException.class, null);

        assertContains(null, ex.getMessage(), "AtomicLong with name '" + name + "' does not exist.");
        assertEquals(ClientStatus.RESOURCE_DOES_NOT_EXIST, ((ClientServerError)ex.getCause()).getCode());
    }
}
