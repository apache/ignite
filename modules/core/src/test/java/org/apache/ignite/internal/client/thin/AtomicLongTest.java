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

import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

import java.util.concurrent.Callable;
import org.apache.ignite.client.ClientAtomicConfiguration;
import org.apache.ignite.client.ClientAtomicLong;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.junit.Test;

/**
 * Tests client atomic long.
 */
public class AtomicLongTest extends AbstractThinClientTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    @Test
    public void testCreateSetsInitialValue() {
        String name = "testCreateSetsInitialValue";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicLong atomicLong = client.atomicLong(name, 42, true);

            ClientAtomicLong atomicLongWithGroup = client.atomicLong(
                    name, new ClientAtomicConfiguration().setGroupName("grp"), 43, true);

            assertEquals(42, atomicLong.get());
            assertEquals(43, atomicLongWithGroup.get());
        }
    }

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

    private void assertDoesNotExistError(String name, Callable<Object> callable) {
        ClientException ex = (ClientException) assertThrows(null, callable, ClientException.class, null);

        assertContains(null, ex.getMessage(), "AtomicLong with name '" + name + "' does not exist.");
    }
}
