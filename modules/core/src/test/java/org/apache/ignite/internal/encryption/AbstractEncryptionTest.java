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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionKey;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.WALMode.FSYNC;
import static org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi.CIPHER_ALGO;
import static org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi.DEFAULT_MASTER_KEY_NAME;

/**
 * Abstract encryption test.
 */
public abstract class AbstractEncryptionTest extends GridCommonAbstractTest {
    /** */
    static final String ENCRYPTED_CACHE = "encrypted";

    /** */
    public static final String KEYSTORE_PATH =
        IgniteUtils.resolveIgnitePath("modules/core/src/test/resources/tde.jks").getAbsolutePath();

    /** */
    static final String GRID_0 = "grid-0";

    /** */
    static final String GRID_1 = "grid-1";

    /** */
    public static final String KEYSTORE_PASSWORD = "love_sex_god";

    /** */
    public static final String MASTER_KEY_NAME_2 = "ignite.master.key2";

    /** */
    public static final String MASTER_KEY_NAME_3 = "ignite.master.key3";

    /** */
    public static final String MASTER_KEY_NAME_MULTIBYTE_ENCODED = "мастер.ключ.1";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        KeystoreEncryptionSpi encSpi = new KeystoreEncryptionSpi();

        encSpi.setKeyStorePath(keystorePath());
        encSpi.setKeyStorePassword(keystorePassword());

        cfg.setEncryptionSpi(encSpi);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(10L * 1024 * 1024)
                    .setPersistenceEnabled(true))
            .setPageSize(4 * 1024)
            .setWalMode(FSYNC);

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** */
    protected char[] keystorePassword() {
        return KEYSTORE_PASSWORD.toCharArray();
    }

    /** */
    protected String keystorePath() {
        return KEYSTORE_PATH;
    }

    /** */
    void checkEncryptedCaches(IgniteEx grid0, IgniteEx grid1) {
        Set<String> cacheNames = new HashSet<>(grid0.cacheNames());

        cacheNames.addAll(grid1.cacheNames());

        for (String cacheName : cacheNames) {
            CacheConfiguration ccfg = grid1.cache(cacheName).getConfiguration(CacheConfiguration.class);

            if (!ccfg.isEncryptionEnabled())
                continue;

            IgniteInternalCache<?, ?> encrypted0 = grid0.cachex(cacheName);

            int grpId = CU.cacheGroupId(cacheName, ccfg.getGroupName());

            assertNotNull(encrypted0);

            IgniteInternalCache<?, ?> encrypted1 = grid1.cachex(cacheName);

            assertNotNull(encrypted1);

            assertTrue(encrypted1.configuration().isEncryptionEnabled());

            KeystoreEncryptionKey encKey0 = (KeystoreEncryptionKey)grid0.context().encryption().groupKey(grpId);

            assertNotNull(encKey0);
            assertNotNull(encKey0.key());

            if (!grid1.configuration().isClientMode()) {
                KeystoreEncryptionKey encKey1 = (KeystoreEncryptionKey)grid1.context().encryption().groupKey(grpId);

                assertNotNull(encKey1);
                assertNotNull(encKey1.key());

                assertEquals(encKey0.key(), encKey1.key());
            }
            else
                assertNull(grid1.context().encryption().groupKey(grpId));
        }

        checkData(grid0);
    }

    /** */
    protected void checkData(IgniteEx grid0) {
        IgniteCache<Long, String> cache = grid0.cache(cacheName());

        assertNotNull(cache);

        for (long i = 0; i < 100; i++)
            assertEquals("" + i, cache.get(i));
    }

    /** */
    protected void createEncryptedCache(IgniteEx grid0, @Nullable IgniteEx grid1, String cacheName, String cacheGroup)
        throws IgniteInterruptedCheckedException {
        createEncryptedCache(grid0, grid1, cacheName, cacheGroup, true);
    }

    /** */
    protected void createEncryptedCache(IgniteEx grid0, @Nullable IgniteEx grid1, String cacheName, String cacheGroup,
        boolean putData) throws IgniteInterruptedCheckedException {
        CacheConfiguration<Long, String> ccfg = new CacheConfiguration<Long, String>(cacheName)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setGroupName(cacheGroup)
            .setEncryptionEnabled(true);

        IgniteCache<Long, String> cache = grid0.createCache(ccfg);

        if (grid1 != null)
            GridTestUtils.waitForCondition(() -> grid1.cachex(cacheName()) != null, 2_000L);

        if (putData) {
            for (long i = 0; i < 100; i++)
                cache.put(i, "" + i);

            for (long i = 0; i < 100; i++)
                assertEquals("" + i, cache.get(i));
        }
    }

    /**
     * Starts tests grid instances.
     *
     * @param clnPersDir If {@code true} than before start persistence dir are cleaned.
     * @return Started grids.
     * @throws Exception If failed.
     */
    protected T2<IgniteEx, IgniteEx> startTestGrids(boolean clnPersDir) throws Exception {
        if (clnPersDir)
            cleanPersistenceDir();

        IgniteEx grid0 = startGrid(GRID_0);

        IgniteEx grid1 = startGrid(GRID_1);

        grid0.cluster().active(true);

        awaitPartitionMapExchange();

        return new T2<>(grid0, grid1);
    }

    /** */
    @NotNull protected String cacheName() {
        return ENCRYPTED_CACHE;
    }

    /**
     * Method to create new keystore.
     * Use it whenever you need special keystore for an encryption tests.
     */
    @SuppressWarnings("unused")
    protected File createKeyStore(String keystorePath) throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");

        ks.load(null, null);

        KeyGenerator gen = KeyGenerator.getInstance(CIPHER_ALGO);

        gen.init(KeystoreEncryptionSpi.DEFAULT_KEY_SIZE);

        String[] keyNames = {DEFAULT_MASTER_KEY_NAME, MASTER_KEY_NAME_2, MASTER_KEY_NAME_3, MASTER_KEY_NAME_MULTIBYTE_ENCODED};

        for (String name : keyNames) {
            SecretKey key = gen.generateKey();

            ks.setEntry(
                name,
                new KeyStore.SecretKeyEntry(key),
                new KeyStore.PasswordProtection(KEYSTORE_PASSWORD.toCharArray()));
        }

        File keyStoreFile = new File(keystorePath);

        keyStoreFile.createNewFile();

        try (OutputStream os = new FileOutputStream(keyStoreFile)) {
            ks.store(os, KEYSTORE_PASSWORD.toCharArray());
        }

        return keyStoreFile;
    }

    /**
     * @param name Master key name.
     * @return {@code True} if all nodes have the provided master key name.
     */
    protected boolean checkMasterKeyName(String name) {
        for (Ignite grid : G.allGrids())
            if (!((IgniteEx)grid).context().clientNode() && !name.equals(grid.encryption().getMasterKeyName()))
                return false;

        return true;
    }
}
