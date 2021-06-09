/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util.concurrent;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class AdjustableSemaphoreTest {

    @Test
    public void updateMaxPermitsTest() {
        final AdjustableSemaphore semaphore = new AdjustableSemaphore(5);
        Assert.assertEquals(5, semaphore.availablePermits());
        Assert.assertEquals(5, semaphore.getMaxPermits());
        for (int i = 0; i < 5; i++) {
            Assert.assertTrue(semaphore.tryAcquire());
        }
        Assert.assertFalse(semaphore.tryAcquire());
        Assert.assertEquals(0, semaphore.availablePermits());
        Assert.assertEquals(5, semaphore.getMaxPermits());
        for (int i = 0; i < 5; i++) {
            semaphore.release();
        }
        Assert.assertEquals(5, semaphore.availablePermits());

        // decrease
        semaphore.setMaxPermits(2);
        Assert.assertEquals(2, semaphore.getMaxPermits());
        Assert.assertEquals(2, semaphore.availablePermits());
        for (int i = 0; i < 2; i++) {
            Assert.assertTrue(semaphore.tryAcquire());
        }
        Assert.assertFalse(semaphore.tryAcquire());
        Assert.assertEquals(0, semaphore.availablePermits());
        Assert.assertEquals(2, semaphore.getMaxPermits());

        for (int i = 0; i < 2; i++) {
            semaphore.release();
        }
        // increase
        semaphore.setMaxPermits(10);
        Assert.assertEquals(10, semaphore.getMaxPermits());
        Assert.assertEquals(10, semaphore.availablePermits());
        for (int i = 0; i < 10; i++) {
            Assert.assertTrue(semaphore.tryAcquire());
        }
        Assert.assertFalse(semaphore.tryAcquire());
        Assert.assertEquals(0, semaphore.availablePermits());
        Assert.assertEquals(10, semaphore.getMaxPermits());
    }
}
