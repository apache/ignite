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

import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.vault.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test suite for the {@link VaultManager}.
 */
public class VaultManagerTest {
    /** */
    private static final int TIMEOUT_SECONDS = 1;

    /** */
    private final VaultManager vaultManager = new VaultManager(new InMemoryVaultService());

    /** */
    @AfterEach
    void tearDown() throws Exception {
        vaultManager.close();
    }

    /**
     * Tests the {@link VaultManager#putName} and {@link VaultManager#name} methods.
     */
    @Test
    void testName() throws Exception {
        assertThat(vaultManager.name(), willBe(nullValue(String.class)));

        vaultManager.putName("foobar").get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(vaultManager.name(), willBe(equalTo("foobar")));

        vaultManager.putName("foobarbaz").get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(vaultManager.name(), willBe(equalTo("foobarbaz")));
    }

    /**
     * Tests that {@link VaultManager#putName} does not accept empty strings.
     */
    @Test
    void testEmptyName() {
        assertThrows(
            IllegalArgumentException.class,
            () -> vaultManager.putName("").get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
        );
    }
}
