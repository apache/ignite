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
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.common.Entry;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Test for base vault manager contracts.
 */
public class VaultBaseContractsTest {
    /** Vault. */
    private VaultManager vaultMgr;

    /**
     * Instantiate vault.
     */
    @BeforeEach
    public void setUp() {
        vaultMgr = new VaultManager(new VaultServiceImpl());
    }

    /**
     * put contract
     */
    @Test
    public void put() throws ExecutionException, InterruptedException {
        ByteArray key = getKey(1);
        byte[] val = getValue(key, 1);

        assertNull(vaultMgr.get(key).get().value());

        vaultMgr.put(key, val);

        Entry v = vaultMgr.get(key).get();

        assertFalse(v.empty());
        assertEquals(val, v.value());

        vaultMgr.put(key, val);

        v = vaultMgr.get(key).get();

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

        assertNull(vaultMgr.get(key).get().value());

        // Remove non-existent value.
        vaultMgr.remove(key);

        assertNull(vaultMgr.get(key).get().value());

        vaultMgr.put(key, val);

        Entry v = vaultMgr.get(key).get();

        assertFalse(v.empty());
        assertEquals(val, v.value());

        // Remove existent value.
        vaultMgr.remove(key);

        v = vaultMgr.get(key).get();

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

            assertNull(vaultMgr.get(key).get().value());
        }

        values.forEach((k, v) -> vaultMgr.put(k, v));

        for (Map.Entry<ByteArray, byte[]> entry : values.entrySet())
            assertEquals(entry.getValue(), vaultMgr.get(entry.getKey()).get().value());

        Iterator<Entry> it = vaultMgr.range(getKey(3), getKey(7));

        List<Entry> rangeRes = new ArrayList<>();

        it.forEachRemaining(rangeRes::add);

        assertEquals(4, rangeRes.size());

        //Check that we have exact range from "key3" to "key6"
        for (int i = 3; i < 7; i++)
            assertArrayEquals(values.get(getKey(i)), rangeRes.get(i - 3).value());
    }

    /**
     * putAll with applied revision contract.
     */
    @Test
    public void putAllAndRevision() throws ExecutionException, InterruptedException, IgniteInternalCheckedException {
        Map<ByteArray, byte[]> entries = new HashMap<>();

        int entriesNum = 100;

        ByteArray appRevKey = ByteArray.fromString("test_applied_revision");

        for (int i = 0; i < entriesNum; i++) {
            ByteArray key = getKey(i);

            entries.put(key, getValue(key, i));
        }

        for (int i = 0; i < entriesNum; i++) {
            ByteArray key = getKey(i);

            assertNull(vaultMgr.get(key).get().value());
        }

        vaultMgr.putAll(entries, appRevKey, 1L);

        for (int i = 0; i < entriesNum; i++) {
            ByteArray key = getKey(i);

            assertArrayEquals(entries.get(key), vaultMgr.get(key).get().value());
        }

        assertEquals(1L, ByteUtils.bytesToLong(vaultMgr.get(appRevKey).get().value()));
    }

    /**
     * putAll contract.
     */
    @Test
    public void putAll() throws ExecutionException, InterruptedException {
        Map<ByteArray, byte[]> entries = new HashMap<>();

        int entriesNum = 100;

        for (int i = 0; i < entriesNum; i++) {
            ByteArray key = getKey(i);

            entries.put(key, getValue(key, i));
        }

        for (int i = 0; i < entriesNum; i++) {
            ByteArray key = getKey(i);

            assertNull(vaultMgr.get(key).get().value());
        }

        vaultMgr.putAll(entries);

        for (int i = 0; i < entriesNum; i++) {
            ByteArray key = getKey(i);

            assertArrayEquals(entries.get(key), vaultMgr.get(key).get().value());
        }
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
