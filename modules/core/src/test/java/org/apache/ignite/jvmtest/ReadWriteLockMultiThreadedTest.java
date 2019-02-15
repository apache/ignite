/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.jvmtest;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * JDK read write lock test.
 */
public class ReadWriteLockMultiThreadedTest {
    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    @Test
    public void testReadThenWriteLockAcquire() throws Exception {
        ReadWriteLock lock = new ReentrantReadWriteLock();

        lock.readLock().lock();

        lock.writeLock().lock();
    }

    /**
     *
     */
    @Test
    public void testNotOwnedLockRelease() {
        ReadWriteLock lock = new ReentrantReadWriteLock();

        lock.readLock().unlock();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    @Test
    public void testWriteLockAcquire() throws Exception {
        final ReadWriteLock lock = new ReentrantReadWriteLock();

        lock.readLock().lock();

        X.println("Read lock acquired.");

        IgniteInternalFuture fut1 = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    X.println("Attempting to acquire write lock: " + lock);

                    lock.writeLock().lock();

                    try {
                        X.println("Write lock acquired: " + lock);

                        return null;
                    }
                    finally {
                        lock.writeLock().unlock();
                    }
                }
            },
            1,
            "write-lock"
        );

        Thread.sleep(2000);

        IgniteInternalFuture fut2 = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    X.println("Attempting to acquire read lock: " + lock);

                    lock.readLock().lock();

                    try {
                        X.println("Read lock acquired: " + lock);

                        return null;
                    }
                    finally {
                        lock.readLock().unlock();
                    }
                }
            },
            1,
            "read-lock"
        );

        Thread.sleep(2000);

        X.println(">>> Dump threads now! <<<");

        Thread.sleep(15 * 1000);

        X.println("Read lock released: " + lock);

        lock.readLock().unlock();

        fut1.get();
        fut2.get();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    @Test
    public void testReadLockAcquire() throws Exception {
        final ReadWriteLock lock = new ReentrantReadWriteLock();

        lock.writeLock().lock();

        X.println("Write lock acquired: " + lock);

        IgniteInternalFuture fut = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    X.println("Attempting to acquire read lock: " + lock);

                    lock.readLock().lock();

                    try {
                        X.println("Read lock acquired: " + lock);

                        return null;
                    }
                    finally {
                        lock.readLock().unlock();
                    }
                }
            },
            1,
            "read-lock"
        );

        Thread.sleep(2000);

        X.println(">>> Dump threads now! <<<");

        Thread.sleep(15 * 1000);

        X.println("Write lock released.");

        lock.writeLock().unlock();

        fut.get();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    @Test
    public void testTryWriteLock() throws Exception {
        final ReadWriteLock lock = new ReentrantReadWriteLock();

        lock.readLock().lock();

        X.println("Read lock acquired.");

        IgniteInternalFuture fut = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    boolean res = lock.writeLock().tryLock();

                    X.println("Attempting to try write lock: " + res);

                    try {
                        return null;
                    }
                    finally {
                        if (res)
                            lock.writeLock().unlock();
                    }
                }
            },
            1,
            "write-lock"
        );

        Thread.sleep(2000);

        X.println("Read lock released: " + lock);

        lock.readLock().unlock();

        fut.get();
    }
}
