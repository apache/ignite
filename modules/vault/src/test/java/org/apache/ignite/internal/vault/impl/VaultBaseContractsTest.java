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

package org.apache.ignite.internal.vault.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.common.Entry;
import org.apache.ignite.internal.vault.common.VaultListener;
import org.apache.ignite.internal.vault.common.VaultWatch;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for base vault manager contracts.
 */
public class VaultBaseContractsTest {
    /** Vault. */
    private VaultManager vaultManager;

    /**
     * Instantiate vault.
     */
    @BeforeEach
    public void setUp() {
        vaultManager = new VaultManager(new VaultServiceImpl());
    }

    /**
     * put contract
     */
    @Test
    public void put() throws ExecutionException, InterruptedException {
        ByteArray key = getKey(1);
        byte[] val = getValue(key, 1);

        assertNull(vaultManager.get(key).get().value());

        vaultManager.put(key, val);

        Entry v = vaultManager.get(key).get();

        assertFalse(v.empty());
        assertEquals(val, v.value());

        vaultManager.put(key, val);

        v = vaultManager.get(key).get();

        assertFalse(v.empty());
        assertEquals(val, v.value());
    }

    /**
     * remove contract.
     */
    @Test
    public void remove() throws ExecutionException, InterruptedException {
        ByteArray key = getKey(1);
        byte[] val = getValue(key, 1);

        assertNull(vaultManager.get(key).get().value());

        // Remove non-existent value.
        vaultManager.remove(key);

        assertNull(vaultManager.get(key).get().value());

        vaultManager.put(key, val);

        Entry v = vaultManager.get(key).get();

        assertFalse(v.empty());
        assertEquals(val, v.value());

        // Remove existent value.
        vaultManager.remove(key);

        v = vaultManager.get(key).get();

        assertNull(v.value());
    }

    /**
     * range contract.
     */
    @Test
    public void range() throws ExecutionException, InterruptedException {
        ByteArray key;

        Map<ByteArray, byte[]> values = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            key = getKey(i);

            values.put(key, getValue(key, i));

            assertNull(vaultManager.get(key).get().value());
        }

        values.forEach((k, v) -> vaultManager.put(k, v));

        for (Map.Entry<ByteArray, byte[]> entry : values.entrySet())
            assertEquals(entry.getValue(), vaultManager.get(entry.getKey()).get().value());

        Iterator<Entry> it = vaultManager.range(getKey(3), getKey(7));

        List<Entry> rangeRes = new ArrayList<>();

        it.forEachRemaining(rangeRes::add);

        assertEquals(4, rangeRes.size());

        //Check that we have exact range from "key3" to "key6"
        for (int i = 3; i < 7; i++)
            assertArrayEquals(values.get(getKey(i)), rangeRes.get(i - 3).value());
    }

    /**
     * watch contract.
     */
    @Test
    public void watch() throws ExecutionException, InterruptedException {
        ByteArray key;

        Map<ByteArray, byte[]> values = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            key = getKey(i);

            values.put(key, getValue(key, i));
        }

        values.forEach((k, v) -> vaultManager.put(k, v));

        for (Map.Entry<ByteArray, byte[]> entry : values.entrySet())
            assertEquals(entry.getValue(), vaultManager.get(entry.getKey()).get().value());

        CountDownLatch counter = new CountDownLatch(4);

        VaultWatch vaultWatch = new VaultWatch(getKey(3), getKey(7), new VaultListener() {
            @Override public boolean onUpdate(@NotNull Iterable<Entry> entries) {
                counter.countDown();

                return true;
            }

            @Override public void onError(@NotNull Throwable e) {
                // no-op
            }
        });

        vaultManager.watch(vaultWatch);

        for (int i = 3; i < 7; i++)
            vaultManager.put(getKey(i), ("new" + i).getBytes());

        assertTrue(counter.await(10, TimeUnit.MILLISECONDS));
    }

    /**
     * putAll contract.
     */
    @Test
    public void putAllAndRevision() throws ExecutionException, InterruptedException, IgniteInternalCheckedException {
        Map<ByteArray, byte[]> entries = new HashMap<>();

        int entriesNum = 100;

        for (int i = 0; i < entriesNum; i++) {
            ByteArray key = getKey(i);

            entries.put(key, getValue(key, i));
        }

        for (int i = 0; i < entriesNum; i++) {
            ByteArray key = getKey(i);

            assertNull(vaultManager.get(key).get().value());
        }

        vaultManager.putAll(entries, 1L);

        for (int i = 0; i < entriesNum; i++) {
            ByteArray key = getKey(i);

            assertEquals(entries.get(key), vaultManager.get(key).get().value());
        }

        assertEquals(1L, vaultManager.appliedRevision());
    }

    /**
     * Creates key for vault entry.
     */
    private static ByteArray getKey(int k) {
        return ByteArray.fromString("key" + k);
    }

    /**
     * Creates value represented by byte array.
     */
    private static byte[] getValue(ByteArray k, int v) {
        return ("key" + k + '_' + "val" + v).getBytes();
    }
}
