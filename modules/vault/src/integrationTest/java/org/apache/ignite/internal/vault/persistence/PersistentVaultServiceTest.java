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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultService;
import org.apache.ignite.internal.vault.VaultServiceTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

/**
 * Test suite for the {@link PersistentVaultService}.
 */
class PersistentVaultServiceTest extends VaultServiceTest {
    /** */
    private Path baseDir;

    /** */
    private Path vaultDir;

    /** */
    @BeforeEach
    @Override public void setUp(TestInfo testInfo) throws IOException {
        baseDir = testInfo.getTestMethod()
            .map(Method::getName)
            .map(Paths::get)
            .orElseThrow();

        vaultDir = baseDir.resolve("vault");

        Files.createDirectories(vaultDir);

        super.setUp(testInfo);
    }

    /** {@inheritDoc} */
    @AfterEach
    @Override public void tearDown() throws Exception {
        super.tearDown();

        IgniteUtils.delete(baseDir);
    }

    /** {@inheritDoc} */
    @Override protected VaultService getVaultService() {
        return new PersistentVaultService(vaultDir);
    }
}
