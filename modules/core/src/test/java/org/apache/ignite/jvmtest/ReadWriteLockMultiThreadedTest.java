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

package org.apache.ignite.jvmtest;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import junit.framework.TestCase;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;

/**
 * JDK read write lock test.
 */
public class ReadWriteLockMultiThreadedTest extends TestCase {
    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    public void testReadThenWriteLockAcquire() throws Exception {
        ReadWriteLock lock = new ReentrantReadWriteLock();

        lock.readLock().lock();

        lock.writeLock().lock();
    }

    /**
     *
     */
    public void testNotOwnedLockRelease() {
        ReadWriteLock lock = new ReentrantReadWriteLock();

        lock.readLock().unlock();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
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