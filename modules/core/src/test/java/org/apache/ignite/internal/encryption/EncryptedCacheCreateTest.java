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

package org.apache.ignite.internal.encryption;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import com.google.common.primitives.Bytes;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionKey;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/** */
public class EncryptedCacheCreateTest extends AbstractEncryptionTest {
    /** Non-persistent data region name. */
    private static final String NO_PERSISTENCE_REGION = "no-persistence-region";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        IgniteEx igniteEx = startGrid(0);

        startGrid(1);

        igniteEx.cluster().active(true);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        DataStorageConfiguration memCfg = cfg.getDataStorageConfiguration();

        memCfg.setDataRegionConfigurations(new DataRegionConfiguration()
            .setMaxSize(10L * 1024 * 1024)
            .setName(NO_PERSISTENCE_REGION)
            .setPersistenceEnabled(false));

        return cfg;
    }

    /** @throws Exception If failed. */
    @Test
    public void testCreateEncryptedCache() throws Exception {
        CacheConfiguration<Long, String> ccfg = new CacheConfiguration<>(ENCRYPTED_CACHE);

        ccfg.setEncryptionEnabled(true);

        IgniteEx grid = grid(0);

        grid.createCache(ccfg);

        IgniteInternalCache<Object, Object> enc = grid.cachex(ENCRYPTED_CACHE);

        assertNotNull(enc);

        KeystoreEncryptionKey key =
            (KeystoreEncryptionKey)grid.context().encryption().groupKey(CU.cacheGroupId(ENCRYPTED_CACHE, null));

        assertNotNull(key);
        assertNotNull(key.key());
    }

    /** @throws Exception If failed. */
    @Test
    public void testCreateEncryptedNotPersistedCacheFail() throws Exception {
        GridTestUtils.assertThrowsWithCause(() -> {
            CacheConfiguration<Long, String> ccfg = new CacheConfiguration<>(NO_PERSISTENCE_REGION);

            ccfg.setEncryptionEnabled(true);
            ccfg.setDataRegionName(NO_PERSISTENCE_REGION);

            grid(0).createCache(ccfg);
        }, IgniteCheckedException.class);
    }

    /** @throws Exception If failed. */
    @Test
    public void testPersistedContentEncrypted() throws Exception {
        IgniteCache<Integer, String> enc = grid(0).createCache(
            new CacheConfiguration<Integer, String>(ENCRYPTED_CACHE)
                .setEncryptionEnabled(true));

        IgniteCache<Integer, String> plain = grid(0).createCache(new CacheConfiguration<>("plain-cache"));

        assertNotNull(enc);

        String encValPart = "AAAAAAAAAA";
        String plainValPart = "BBBBBBBBBB";

        StringBuilder longEncVal = new StringBuilder(encValPart.length() * 100);
        StringBuilder longPlainVal = new StringBuilder(plainValPart.length() * 100);

        for (int i = 0; i < 100; i++) {
            longEncVal.append(encValPart);
            longPlainVal.append(plainValPart);
        }

        enc.put(1, longEncVal.toString());
        plain.put(1, longPlainVal.toString());

        stopAllGrids(false);

        byte[] encValBytes = encValPart.getBytes(StandardCharsets.UTF_8);
        byte[] plainValBytes = plainValPart.getBytes(StandardCharsets.UTF_8);

        final boolean[] plainBytesFound = {false};

        Files.walk(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false).toPath())
            .filter(Files::isRegularFile)
            .forEach(f -> {
                try {
                    if (Files.size(f) == 0)
                        return;

                    byte[] fileBytes = Files.readAllBytes(f);

                    boolean notFound = Bytes.indexOf(fileBytes, encValBytes) == -1;

                    assertTrue("Value should be encrypted in persisted file. " + f.getFileName(), notFound);

                    plainBytesFound[0] = plainBytesFound[0] || Bytes.indexOf(fileBytes, plainValBytes) != -1;

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

        assertTrue("Plain value should be found in persistent store", plainBytesFound[0]);
    }
}
