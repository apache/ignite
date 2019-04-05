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

package org.apache.ignite.internal;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.datastructures.GridCacheAtomicReferenceImpl;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 *
 */
public class IgniteClientReconnectAtomicsWithLostPartitionsTest extends IgniteClientReconnectAbstractTest {
    /** {@inheritDoc} */
    @Override protected int serverCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected int clientCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        String consistentId = "consistent-id-" + igniteInstanceName.charAt(igniteInstanceName.length() - 1);

        cfg.setConsistentId(consistentId);

        AtomicConfiguration atomicCfg = new AtomicConfiguration()
            .setBackups(0)
            .setAffinity(new RendezvousAffinityFunction(false, 32));

        cfg.setAtomicConfiguration(atomicCfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicLongGet() throws Exception {
        testAtomicLongReconnectClusterRestart("atomic-long-get", IgniteAtomicLong::get);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicLongIncrementAndGet() throws Exception {
        testAtomicLongReconnectClusterRestart("atomic-long-incrementAndGet", IgniteAtomicLong::incrementAndGet);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicLongAddAndGet() throws Exception {
        testAtomicLongReconnectClusterRestart("atomic-long-addAndGet", atomic -> atomic.addAndGet(1L));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicLongGetAndAdd() throws Exception {
        testAtomicLongReconnectClusterRestart("atomic-long-getAndAdd", atomic -> atomic.getAndAdd(1L));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicLongDecrementAndGet() throws Exception {
        testAtomicLongReconnectClusterRestart("atomic-long-decrementAndGet", IgniteAtomicLong::decrementAndGet);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicLongGetAndDecrement() throws Exception {
        testAtomicLongReconnectClusterRestart("atomic-long-getAndDecrement", IgniteAtomicLong::getAndDecrement);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicLongGetAndSet() throws Exception {
        testAtomicLongReconnectClusterRestart("atomic-long-getAndSet", atomic -> atomic.getAndSet(1L));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicLongCompareAndSet() throws Exception {
        testAtomicLongReconnectClusterRestart(
            "atomic-long-compareAndSet",
            atomic -> atomic.compareAndSet(1L, 2L));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicLongGetAndIncrement() throws Exception {
        testAtomicLongReconnectClusterRestart("atomic-long-getAndIncrement", IgniteAtomicLong::getAndIncrement);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicReferenceGet() throws Exception {
        testAtomicReferenceReconnectClusterRestart("atomic-ref-get", IgniteAtomicReference::get);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicReferenceSet() throws Exception {
        testAtomicReferenceReconnectClusterRestart("atomic-ref-set", atomic -> atomic.set(50L));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicReferenceCompareAndSet() throws Exception {
        testAtomicReferenceReconnectClusterRestart(
            "atomic-ref-compareAndSet",
            atomic -> atomic.compareAndSet(1L, 50L));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicReferenceCompareAndSetAndGet() throws Exception {
        testAtomicReferenceReconnectClusterRestart(
            "atomic-ref-compareAndSetAndGet",
            atomic -> ((GridCacheAtomicReferenceImpl)atomic).compareAndSetAndGet(1L, 50L));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicStampedGet() throws Exception {
        testAtomicStampedReconnectClusterRestart(
            "atomic-stamped-get",
            IgniteAtomicStamped::get);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicStampedSet() throws Exception {
        testAtomicStampedReconnectClusterRestart(
            "atomic-stamped-set",
            atomic -> atomic.set("val", "stamp"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicStampedCompareAndSet() throws Exception {
        testAtomicStampedReconnectClusterRestart(
            "atomic-stamped-compareAndSet",
            atomic -> atomic.compareAndSet("val", "stamp", "val", "stamp"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicStampedStamp() throws Exception {
        testAtomicStampedReconnectClusterRestart(
            "atomic-stamped-stamp",
            IgniteAtomicStamped::stamp);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicStampedValue() throws Exception {
        testAtomicStampedReconnectClusterRestart(
            "atomic-stamped-val",
            IgniteAtomicStamped::value);
    }

    /**
     * Tests atomic long operation provided by the the given {@code clo}.
     *
     * @param atomicName Name of atomic long.
     * @param clo Closure that represents an operation.
     * @throws Exception If failed.
     */
    private void testAtomicLongReconnectClusterRestart(
        String atomicName,
        final IgniteInClosure<IgniteAtomicLong> clo
    ) throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        final IgniteAtomicLong atomic = client.atomicLong(atomicName, 1L, true);

        assertNotNull(atomic);

        assertEquals("Unexpected initial value.", 1L, atomic.get());

        // Restart the cluster without waiting for rebalancing.
        // It should lead to data loss because there are no backups in the atomic configuration.
        for (int i = 0; i < serverCount(); ++i) {
            grid(i).close();

            startGrid(i);
        }

        GridTestUtils.assertThrows(
            log,
            () -> {
                clo.apply(atomic);

                return null;
            },
            IgniteException.class,
            "Failed to find atomic long: " + atomicName);

        assertTrue("Atomic long instance should be removed.", atomic.removed());

        IgniteAtomicLong recreatedAtomicLong = client.atomicLong(atomicName, 100L, true);

        assertEquals("Unexpected initial value.", 100L, recreatedAtomicLong.get());
    }

    /**
     * Tests atomic reference operation provided by the the given {@code clo}.
     *
     * @param atomicName Name of atomic.
     * @param clo Closure that represents an operation.
     * @throws Exception If failed.
     */
    private void testAtomicReferenceReconnectClusterRestart(
        String atomicName,
        final IgniteInClosure<IgniteAtomicReference<Long>> clo
    ) throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        final IgniteAtomicReference atomic = client.atomicReference(atomicName, 1L, true);

        assertNotNull(atomic);

        assertEquals("Unexpected initial value.", 1L, atomic.get());

        // Restart the cluster without waiting for rebalancing.
        // It should lead to data loss because there are no backups in the atomic configuration.
        for (int i = 0; i < serverCount(); ++i) {
            grid(i).close();

            startGrid(i);
        }

        GridTestUtils.assertThrows(
            log,
            () -> {
                clo.apply(atomic);

                return null;
            },
            IgniteException.class,
            "Failed to find atomic reference with given name: " + atomicName);

        assertTrue("Atomic instance should be removed.", atomic.removed());

        IgniteAtomicReference recreatedAtomic = client.atomicReference(atomicName, 100L, true);

        assertEquals("Unexpected initial value.", 100L, recreatedAtomic.get());
    }

    /**
     * Tests atomic stamped operation provided by the the given {@code clo}.
     *
     * @param atomicName Name of atomic.
     * @param op Closure that represents an operation.
     * @throws Exception If failed.
     */
    private void testAtomicStampedReconnectClusterRestart(
        String atomicName,
        final IgniteInClosure<IgniteAtomicStamped> op
    ) throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        String initVal = "qwerty";
        String initStamp = "asdfgh";

        final IgniteAtomicStamped<String, String> atomic = client.atomicStamped(atomicName, initVal, initStamp, true);

        assertNotNull(atomic);

        assertEquals(initVal, atomic.value());
        assertEquals(initStamp, atomic.stamp());
        assertEquals(initVal, atomic.get().get1());
        assertEquals(initStamp, atomic.get().get2());

        // Restart the cluster without waiting for rebalancing.
        // It should lead to data loss because there are no backups in the atomic configuration.
        for (int i = 0; i < serverCount(); ++i) {
            grid(i).close();

            startGrid(i);
        }

        GridTestUtils.assertThrows(
            log,
            () -> {
                op.apply(atomic);

                return null;
            },
            IgniteException.class,
            "Failed to find atomic stamped with given name: " + atomicName);

        assertTrue("Atomic instance should be removed.", atomic.removed());

        IgniteAtomicStamped<String, String> recreatedAtomic = client.atomicStamped(atomicName, initVal, initStamp, true);

        assertNotNull(recreatedAtomic);

        assertEquals(initVal, recreatedAtomic.value());
        assertEquals(initStamp, recreatedAtomic.stamp());
        assertEquals(initVal, recreatedAtomic.get().get1());
        assertEquals(initStamp, recreatedAtomic.get().get2());
    }
}
