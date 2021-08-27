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
package org.apache.ignite.raft.jraft.util;

import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class RecyclersTest {

    private static Recyclers<RecyclableObject> newRecyclers(final int max) {
        return new Recyclers<RecyclableObject>(max) {

            @Override
            protected RecyclableObject newObject(final Recyclers.Handle handle) {
                return new RecyclableObject(handle);
            }
        };
    }

    @Test
    public void testMultipleRecycle() {
        final Recyclers<RecyclableObject> recyclers = newRecyclers(16);
        final RecyclableObject object = recyclers.get();
        recyclers.recycle(object, object.handle);
        assertThrows(IllegalStateException.class, () -> recyclers.recycle(object, object.handle));
    }

    @Test
    public void testMultipleRecycleAtDifferentThread() throws InterruptedException {
        final Recyclers<RecyclableObject> recyclers = newRecyclers(512);
        final RecyclableObject object = recyclers.get();
        final Thread thread1 = new Thread(() -> recyclers.recycle(object, object.handle));
        try {
            thread1.start();
        } finally {
            thread1.join();
        }
        assertSame(object, recyclers.get());
    }

    @Test
    public void testRecycleMoreThanOnceAtDifferentThread() throws InterruptedException {
        final Recyclers<RecyclableObject> recyclers = newRecyclers(1024);
        final RecyclableObject object = recyclers.get();

        final AtomicReference<IllegalStateException> exceptionStore = new AtomicReference<>();
        final Thread thread1 = new Thread(() -> recyclers.recycle(object, object.handle));
        try {
            thread1.start();
        } finally {
            thread1.join();
        }

        final Thread thread2 = new Thread(() -> {
            try {
                recyclers.recycle(object, object.handle);
            }
            catch (IllegalStateException e) {
                exceptionStore.set(e);
            }
        });
        try {
            thread2.start();
        } finally {
            thread2.join();
        }

        assertNotNull(exceptionStore.get());
    }

    @Test
    public void testRecycle() {
        final Recyclers<RecyclableObject> recyclers = newRecyclers(16);
        final RecyclableObject object = recyclers.get();
        recyclers.recycle(object, object.handle);
        final RecyclableObject object2 = recyclers.get();
        assertSame(object, object2);
        recyclers.recycle(object2, object2.handle);
    }

    @Test
    public void testRecycleDisable() {
        final Recyclers<RecyclableObject> recyclers = newRecyclers(-1);
        final RecyclableObject object = recyclers.get();
        recyclers.recycle(object, object.handle);
        final RecyclableObject object2 = recyclers.get();
        assertNotSame(object, object2);
        recyclers.recycle(object2, object2.handle);
    }

    @Test
    public void testMaxCapacity() {
        testMaxCapacity(300);
        Random rand = new Random();
        for (int i = 0; i < 50; i++) {
            testMaxCapacity(rand.nextInt(1000) + 256); // 256 - 1256
        }
    }

    private static void testMaxCapacity(final int maxCapacity) {
        final Recyclers<RecyclableObject> recyclers = newRecyclers(maxCapacity);
        final RecyclableObject[] objects = new RecyclableObject[maxCapacity * 3];
        for (int i = 0; i < objects.length; i++) {
            objects[i] = recyclers.get();
        }

        for (int i = 0; i < objects.length; i++) {
            recyclers.recycle(objects[i], objects[i].handle);
            objects[i] = null;
        }

        assertTrue(
            maxCapacity >= recyclers.threadLocalCapacity(),
            "The threadLocalCapacity (" + recyclers.threadLocalCapacity() + ") must be <= maxCapacity ("
                + maxCapacity + ") as we not pool all new handles internally"
        );
    }

    static final class RecyclableObject {

        private final Recyclers.Handle handle;

        private RecyclableObject(Recyclers.Handle handle) {
            this.handle = handle;
        }

        public Recyclers.Handle getHandle() {
            return handle;
        }
    }
}
