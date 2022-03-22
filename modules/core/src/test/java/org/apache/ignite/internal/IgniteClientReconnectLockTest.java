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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.internal.processors.cache.CacheStoppedException;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * The test check that lock properly work after client node reconnected.
 */
public class IgniteClientReconnectLockTest extends IgniteClientReconnectAbstractTest {
    /**
     * {@inheritDoc}
     */
    @Override protected int serverCount() {
        return 2;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected int clientCount() {
        return 1;
    }

    /**
     * Check that lock can be acquired concurrently only one time after client node reconnected.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLockAfterClientReconnected() throws Exception {
        IgniteEx client = grid(serverCount());

        IgniteLock lock = client.reentrantLock("testLockAfterClientReconnected", true, true, true);

        //need to initialize lock instance before client reconnect
        lock.lock();
        lock.unlock();

        reconnectClientNode(client,
            clientRouter(client),
            () -> GridTestUtils.assertThrowsWithCause(() -> lock.lock(), IgniteClientDisconnectedException.class)
        );

        checkLockWorking(lock);
    }

    /**
     * Check that lock can be acquired concurrently only one time after server node restart and client node reconnected
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLockAfterServerRestartAndClientReconnected() throws Exception {
        IgniteEx client = grid(2);

        IgniteLock lock = client.reentrantLock("testLockAfterServerRestartAndClientReconnected", true, true, true);

        //need to initialize lock instance before client reconnect
        lock.lock();
        lock.unlock();

        String clientConnected = clientRouter(client).name();

        stopGrid(clientConnected);
        startGrid(clientConnected);

        String clientConnect = clientRouter(client).name();

        assertTrue(!clientConnected.equals(clientConnect) && clientConnect != null);

        checkLockWorking(lock);
    }

    /**
     * @param lock Lock.
     * @throws Exception If failed.
     */
    private void checkLockWorking(IgniteLock lock) throws Exception {
        CountDownLatch lockLatch = new CountDownLatch(1);

        AtomicReference<Boolean> tryLockRes = new AtomicReference<>();

        IgniteInternalFuture<?> fut1 = GridTestUtils.runAsync(() -> {
            boolean awaited;

            lock.lock();

            try {
                lockLatch.countDown();

                awaited = GridTestUtils.waitForCondition(() -> tryLockRes.get() != null, 5000);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new RuntimeException(e);
            }
            finally {
                lock.unlock();
            }

            assertTrue("Condition was not achived.", awaited);
        });

        IgniteInternalFuture<?> fut2 = GridTestUtils.runAsync(() -> {
            try {
                lockLatch.await();

                tryLockRes.set(lock.tryLock());
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            finally {
                if (lock.isHeldByCurrentThread())
                    lock.unlock();
            }

            assertFalse(tryLockRes.get());
        });

        fut1.get();
        fut2.get();
    }

    /**
     * Check that lock cannot be acquired after cluster restart.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLockAfterClusterRestartAndClientReconnected() throws Exception {
        IgniteEx client = grid(serverCount());

        IgniteLock lock = client.reentrantLock("testLockAfterClusterRestartAndClientReconnected", true, true, true);

        //need to initialize lock instance before client reconnect
        lock.lock();
        lock.unlock();

        stopGrid(0);
        stopGrid(1);
        startGrid(0);
        startGrid(1);

        GridTestUtils.assertThrowsWithCause(() -> {
            try {
                lock.lock();
            }
            catch (IgniteClientDisconnectedException e) {
                e.reconnectFuture().get();

                lock.lock();
            }
        }, CacheStoppedException.class);
    }
}
