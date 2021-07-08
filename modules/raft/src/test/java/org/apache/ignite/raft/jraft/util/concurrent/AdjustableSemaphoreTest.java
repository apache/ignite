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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class AdjustableSemaphoreTest {

    @Test
    public void updateMaxPermitsTest() {
        final AdjustableSemaphore semaphore = new AdjustableSemaphore(5);
        assertEquals(5, semaphore.availablePermits());
        assertEquals(5, semaphore.getMaxPermits());
        for (int i = 0; i < 5; i++) {
            assertTrue(semaphore.tryAcquire());
        }
        assertFalse(semaphore.tryAcquire());
        assertEquals(0, semaphore.availablePermits());
        assertEquals(5, semaphore.getMaxPermits());
        for (int i = 0; i < 5; i++) {
            semaphore.release();
        }
        assertEquals(5, semaphore.availablePermits());

        // decrease
        semaphore.setMaxPermits(2);
        assertEquals(2, semaphore.getMaxPermits());
        assertEquals(2, semaphore.availablePermits());
        for (int i = 0; i < 2; i++) {
            assertTrue(semaphore.tryAcquire());
        }
        assertFalse(semaphore.tryAcquire());
        assertEquals(0, semaphore.availablePermits());
        assertEquals(2, semaphore.getMaxPermits());

        for (int i = 0; i < 2; i++) {
            semaphore.release();
        }
        // increase
        semaphore.setMaxPermits(10);
        assertEquals(10, semaphore.getMaxPermits());
        assertEquals(10, semaphore.availablePermits());
        for (int i = 0; i < 10; i++) {
            assertTrue(semaphore.tryAcquire());
        }
        assertFalse(semaphore.tryAcquire());
        assertEquals(0, semaphore.availablePermits());
        assertEquals(10, semaphore.getMaxPermits());
    }
}
