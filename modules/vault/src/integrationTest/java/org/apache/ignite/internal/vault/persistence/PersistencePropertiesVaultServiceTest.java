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

package org.apache.ignite.internal.vault.persistence;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.lang.ByteArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.apache.ignite.internal.vault.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Test suite for testing persistence properties of {@link PersistentVaultService}.
 */
class PersistencePropertiesVaultServiceTest {
    /** */
    private static final int TIMEOUT_SECONDS = 1;

    /** */
    private Path baseDir;

    /** */
    private Path vaultDir;

    /** */
    @BeforeEach
    void setUp(TestInfo testInfo) throws IOException {
        baseDir = testInfo.getTestMethod()
            .map(Method::getName)
            .map(Paths::get)
            .orElseThrow();

        vaultDir = baseDir.resolve("vault");

        Files.createDirectories(vaultDir);
    }

    /** */
    @AfterEach
    void tearDown() {
        IgniteUtils.delete(baseDir);
    }

    /**
     * Tests that the Vault Service correctly persists data after multiple service restarts.
     */
    @Test
    void testPersistentRestart() throws Exception {
        var data = Map.of(
            new ByteArray("key" + 1), fromString("value" + 1),
            new ByteArray("key" + 2), fromString("value" + 2),
            new ByteArray("key" + 3), fromString("value" + 3)
        );

        try (var vaultService = new PersistentVaultService(vaultDir)) {
            vaultService.putAll(data).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }

        try (var vaultService = new PersistentVaultService(vaultDir)) {
            assertThat(
                vaultService.get(new ByteArray("key" + 1)),
                willBe(equalTo(new VaultEntry(new ByteArray("key" + 1), fromString("value" + 1))))
            );
        }

        try (
            var vaultService = new PersistentVaultService(vaultDir);
            var cursor = vaultService.range(new ByteArray("key" + 1), new ByteArray("key" + 4))
        ) {
            var actualData = new ArrayList<VaultEntry>();

            cursor.forEachRemaining(actualData::add);

            List<VaultEntry> expectedData = data.entrySet().stream()
                .map(e -> new VaultEntry(e.getKey(), e.getValue()))
                .sorted(Comparator.comparing(VaultEntry::key))
                .collect(Collectors.toList());

            assertThat(actualData, is(expectedData));
        }
    }

    /**
     * Converts a {@code String} into a byte array.
     */
    private static byte[] fromString(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }
}
