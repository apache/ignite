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

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.security.KeyStore;
import java.util.HashSet;
import java.util.Set;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.encryption.EncryptionKey;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.encryption.EncryptionSpiImpl;
import org.apache.ignite.spi.encryption.EncryptionSpiImplSelfTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.spi.encryption.EncryptionSpiImpl.CIPHER_ALGO;
import static org.apache.ignite.spi.encryption.EncryptionSpiImpl.MASTER_KEY_NAME;

/**
 */
public abstract class AbstractEncryptionTest extends GridCommonAbstractTest {
    /** */
    public static final String ENCRYPTED_CACHE = "encrypted";

    /** */
    public static final String KEYSTORE_PATH =
        IgniteUtils.resolveIgnitePath("modules/core/src/test/resources/tde.jks").getAbsolutePath();

    /** */
    static final String GRID_0 = "grid-0";

    /** */
    static final String GRID_1 = "grid-1";

    /** */
    public static final String KEYSTORE_PASSWORD = "love_sex_god";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        EncryptionSpiImpl encSpi = new EncryptionSpiImpl();

        encSpi.setKeyStorePath(KEYSTORE_PATH);
        encSpi.setKeyStorePassword(KEYSTORE_PASSWORD.toCharArray());

        cfg.setEncryptionSpi(encSpi);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(10L * 1024 * 1024)
                    .setPersistenceEnabled(true))
            .setPageSize(4 * 1024)
            .setWalMode(LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** */
    void checkEncCaches(IgniteEx grid0, IgniteEx grid1) {
        Set<String> cacheNames = new HashSet<>(grid0.cacheNames());

        cacheNames.addAll(grid1.cacheNames());

        for (String cacheName : cacheNames) {
            if (!grid0.cachex(cacheName).configuration().isEncrypted())
                continue;

            IgniteInternalCache<?, ?> encrypted0 = grid0.cachex(cacheName);

            int grpId = CU.cacheGroupId(cacheName, encrypted0.configuration().getGroupName());

            assertNotNull(encrypted0);

            IgniteInternalCache<?, ?> encrypted1 = grid1.cachex(cacheName);

            assertNotNull(encrypted1);

            assertTrue(encrypted1.configuration().isEncrypted());

            EncryptionKey<?> encKey0 = encrypted0.context().shared().database().groupKey(grpId);

            assertNotNull(encKey0);
            assertNotNull(encKey0.key());

            EncryptionKey<?> encKey1 = encrypted1.context().shared().database().groupKey(grpId);

            assertNotNull(encKey1);
            assertNotNull(encKey1.key());

            assertEquals(encKey0.key(), encKey1.key());
        }

        IgniteCache<Long, String> cache = grid0.cache(cacheName());

        for (long i=0; i<100; i++)
            assertEquals("" + i, cache.get(i));
    }

    /** */
    protected void createEncCache(IgniteEx grid0, IgniteEx grid1, String cacheName, String cacheGroup)
        throws IgniteInterruptedCheckedException {
        CacheConfiguration<Long, String> ccfg = new CacheConfiguration<Long, String>(cacheName)
            .setGroupName(cacheGroup)
            .setEncrypted(true);

        IgniteCache<Long, String> cache = grid0.createCache(ccfg);

        GridTestUtils.waitForCondition(() -> grid1.cachex(cacheName()) != null, 2_000L);

        for (long i=0; i<100; i++)
            cache.put(i, "" + i);
    }

    /**
     * Starts tests grid instances.
     *
     * @param cleanPersistenceDir If {@code true} than before start persistence dir are cleaned.
     * @return Started grids.
     * @throws Exception If failed.
     */
    protected IgniteEx[] startTestGrids(boolean cleanPersistenceDir) throws Exception {
        if (cleanPersistenceDir)
            cleanPersistenceDir();

        IgniteEx grid0 = startGrid(GRID_0);

        IgniteEx grid1 = startGrid(GRID_1);

        grid0.cluster().active(true);

        awaitPartitionMapExchange();

        return new IgniteEx[] {grid0, grid1};
    }

    /** */
    @NotNull protected String cacheName() {
        return ENCRYPTED_CACHE;
    }

    /**
     * Method to create new keystore.
     * Use it whenever you need special keystore for a encryption tests.
     */
    protected File createKeyStore() throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");

        ks.load(null, null);

        KeyGenerator gen = KeyGenerator.getInstance(CIPHER_ALGO);

        gen.init(192);

        SecretKey key = gen.generateKey();

        ks.setEntry(
            MASTER_KEY_NAME,
            new KeyStore.SecretKeyEntry(key),
            new KeyStore.PasswordProtection(KEYSTORE_PASSWORD.toCharArray()));

        File keyStoreFile = File.createTempFile(EncryptionSpiImplSelfTest.class.getSimpleName(), "jks");

        keyStoreFile.createNewFile();

        try (OutputStream os = new FileOutputStream(keyStoreFile)) {
            ks.store(os, KEYSTORE_PASSWORD.toCharArray());
        }

        return keyStoreFile;
    }
}
