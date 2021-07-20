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

package org.apache.ignite.internal.vault;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Base class for testing {@link VaultService} implementations.
 */
public abstract class VaultServiceTest {
    /** */
    private static final int TIMEOUT_SECONDS = 1;

    /** Vault. */
    private VaultService vaultService;

    /** */
    @BeforeEach
    public void setUp() throws IOException {
        vaultService = getVaultService();
    }

    /** */
    @AfterEach
    public void tearDown() throws Exception {
        vaultService.close();
    }

    /**
     * Returns the vault service that will be tested.
     */
    protected abstract VaultService getVaultService();

    /**
     * Tests regular behaviour of the {@link VaultService#put} method.
     */
    @Test
    public void testPut() throws Exception {
        ByteArray key = getKey(1);

        assertThat(vaultService.get(key), willBe(equalTo(new VaultEntry(key, null))));

        byte[] val = getValue(1);

        doAwait(() -> vaultService.put(key, val));

        assertThat(vaultService.get(key), willBe(equalTo(new VaultEntry(key, val))));

        // test idempotency
        doAwait(() -> vaultService.put(key, val));

        assertThat(vaultService.get(key), willBe(equalTo(new VaultEntry(key, val))));
    }

    /**
     * Tests that the {@link VaultService#put} method removes the given {@code key} if {@code value} equalTo {@code null}.
     */
    @Test
    public void testPutWithNull() throws Exception {
        ByteArray key = getKey(1);

        byte[] val = getValue(1);

        doAwait(() -> vaultService.put(key, val));

        assertThat(vaultService.get(key), willBe(equalTo(new VaultEntry(key, val))));

        doAwait(() -> vaultService.put(key, null));

        assertThat(vaultService.get(key), willBe(equalTo(new VaultEntry(key, null))));
    }

    /**
     * Tests regular behaviour of the {@link VaultService#remove} method.
     */
    @Test
    public void testRemove() throws Exception {
        ByteArray key = getKey(1);

        // Remove non-existent value.
        doAwait(() -> vaultService.remove(key));

        assertThat(vaultService.get(key), willBe(equalTo(new VaultEntry(key, null))));

        byte[] val = getValue(1);

        doAwait(() -> vaultService.put(key, val));

        assertThat(vaultService.get(key), willBe(equalTo(new VaultEntry(key, val))));

        // Remove existing value.
        doAwait(() -> vaultService.remove(key));

        assertThat(vaultService.get(key), willBe(equalTo(new VaultEntry(key, null))));
    }

    /**
     * Tests regular behaviour of the {@link VaultService#putAll} method.
     */
    @Test
    public void testPutAll() throws Exception {
        Map<ByteArray, byte[]> batch = IntStream.range(0, 10)
            .boxed()
            .collect(toMap(VaultServiceTest::getKey, VaultServiceTest::getValue));

        doAwait(() -> vaultService.putAll(batch));

        batch.forEach((k, v) -> assertThat(vaultService.get(k), willBe(equalTo(new VaultEntry(k, v)))));

        doAwait(() -> vaultService.putAll(batch));

        batch.forEach((k, v) -> assertThat(vaultService.get(k), willBe(equalTo(new VaultEntry(k, v)))));
    }

    /**
     * Tests that the {@link VaultService#putAll} method will remove keys, which values are {@code null}.
     */
    @Test
    public void testPutAllWithNull() throws Exception {
        Map<ByteArray, byte[]> batch = IntStream.range(0, 10)
            .boxed()
            .collect(toMap(VaultServiceTest::getKey, VaultServiceTest::getValue));

        doAwait(() -> vaultService.putAll(batch));

        batch.forEach((k, v) -> assertThat(vaultService.get(k), willBe(equalTo(new VaultEntry(k, v)))));

        Map<ByteArray, byte[]> secondBatch = new HashMap<>();

        secondBatch.put(getKey(4), getValue(3));
        secondBatch.put(getKey(8), getValue(3));
        secondBatch.put(getKey(1), null);
        secondBatch.put(getKey(3), null);

        doAwait(() -> vaultService.putAll(secondBatch));

        assertThat(vaultService.get(getKey(4)), willBe(equalTo(new VaultEntry(getKey(4), getValue(3)))));
        assertThat(vaultService.get(getKey(8)), willBe(equalTo(new VaultEntry(getKey(8), getValue(3)))));
        assertThat(vaultService.get(getKey(1)), willBe(equalTo(new VaultEntry(getKey(1), null))));
        assertThat(vaultService.get(getKey(3)), willBe(equalTo(new VaultEntry(getKey(3), null))));
    }

    /**
     * Tests regular behaviour of the {@link VaultService#range} method.
     */
    @Test
    public void testRange() throws Exception {
        List<VaultEntry> entries = getRange(0, 10);

        Map<ByteArray, byte[]> batch = entries.stream().collect(toMap(VaultEntry::key, VaultEntry::value));

        doAwait(() -> vaultService.putAll(batch));

        List<VaultEntry> range = range(getKey(3), getKey(7));

        assertThat(range, equalTo(getRange(3, 7)));
    }

    /**
     * Tests that the {@link VaultService#range} returns valid entries when passed a larger range, than the available
     * data.
     */
    @Test
    public void testRangeBoundaries() throws Exception {
        List<VaultEntry> entries = getRange(3, 5);

        Map<ByteArray, byte[]> batch = entries.stream().collect(toMap(VaultEntry::key, VaultEntry::value));

        doAwait(() -> vaultService.putAll(batch));

        List<VaultEntry> range = range(getKey(0), getKey(9));

        assertThat(range, equalTo(entries));
    }

    /**
     * Tests that the {@link VaultService#range} upper bound equalTo not included.
     */
    @Test
    public void testRangeNotIncludedBoundary() throws Exception {
        List<VaultEntry> entries = getRange(3, 5);

        Map<ByteArray, byte[]> batch = entries.stream().collect(toMap(VaultEntry::key, VaultEntry::value));

        doAwait(() -> vaultService.putAll(batch));

        List<VaultEntry> range = range(getKey(3), getKey(4));

        assertThat(range, equalTo(List.of(new VaultEntry(getKey(3), getValue(3)))));
    }

    /**
     * Tests that an empty result equalTo returned when {@link VaultService#range} contains invalid boundaries.
     */
    @Test
    public void testRangeInvalidBoundaries() throws Exception {
        Map<ByteArray, byte[]> batch = getRange(3, 5).stream().collect(toMap(VaultEntry::key, VaultEntry::value));

        doAwait(() -> vaultService.putAll(batch));

        List<VaultEntry> range = range(getKey(4), getKey(1));

        assertThat(range, is(empty()));

        range = range(getKey(4), getKey(4));

        assertThat(range, is(empty()));
    }

    /**
     * Creates a test key.
     */
    private static ByteArray getKey(int k) {
        return new ByteArray("key" + k);
    }

    /**
     * Creates a test value.
     */
    private static byte[] getValue(int v) {
        return ("val" + v).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Creates a range of test Vault entries.
     */
    private List<VaultEntry> getRange(int from, int to) {
        return IntStream.range(from, to)
            .mapToObj(i -> new VaultEntry(getKey(i), getValue(i)))
            .collect(toList());
    }

    /**
     * Performs the given action and waits for the returned future to complete.
     */
    private static void doAwait(Supplier<CompletableFuture<?>> supplier) throws Exception {
        supplier.get().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Exctracts the given range of values from the Vault.
     */
    private List<VaultEntry> range(ByteArray from, ByteArray to) throws Exception {
        var result = new ArrayList<VaultEntry>();

        try (Cursor<VaultEntry> cursor = vaultService.range(from, to)) {
            cursor.forEach(result::add);
        }

        return result;
    }
}
