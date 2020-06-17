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
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.KeyStore;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager;
import org.apache.ignite.internal.managers.encryption.GroupKey;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
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
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
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

        cfg.setConsistentId(name);

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

    /**
     * @param name Cache name.
     * @param grp Cache group name.
     */
    protected <K, V> CacheConfiguration<K, V> cacheConfiguration(String name, String grp) {
        CacheConfiguration<K, V> cfg = new CacheConfiguration<>(name);

        return cfg.setWriteSynchronizationMode(FULL_SYNC)
            .setGroupName(grp)
            .setEncryptionEnabled(true);
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

            GroupKey grpKey0 = grid0.context().encryption().groupKey(grpId);

            assertNotNull(grpKey0);

            KeystoreEncryptionKey encKey0 = (KeystoreEncryptionKey)grpKey0.key();

            assertNotNull(encKey0);
            assertNotNull(encKey0.key());

            if (!grid1.configuration().isClientMode()) {
                GroupKey grpKey1 = grid1.context().encryption().groupKey(grpId);

                assertNotNull(grpKey1);

                KeystoreEncryptionKey encKey1 = (KeystoreEncryptionKey)grpKey1.key();

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

        int size = cache.size();

        for (long i = 0; i < size; i++)
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
        IgniteCache<Long, String> cache = grid0.createCache(cacheConfiguration(cacheName, cacheGroup));

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

    protected void loadData(int cnt) {
        info("Loading " + cnt + " entries into " + cacheName());

        int start = grid(GRID_0).cache(cacheName()).size();

        try (IgniteDataStreamer<Long, String> streamer = grid(GRID_0).dataStreamer(cacheName())) {
            for (long i = start; i < (cnt + start); i++)
                streamer.addData(i, String.valueOf(i));
        }

        info("Load data finished");
    }

    protected void checkGroupKey(int grpId, int keyId, long timeout) throws IgniteCheckedException, IOException {
        awaitEncryption(G.allGrids(), grpId, timeout);

        for (Ignite g : G.allGrids()) {
            IgniteEx grid = (IgniteEx)g;

            if (grid.context().clientNode())
                continue;

            GridEncryptionManager encryption = grid.context().encryption();

            assertEquals(grid.localNode().id().toString(), (byte)keyId, encryption.groupKey(grpId).id());

            forceCheckpoint(g);

            encryption.encryptionTask(grpId).get();

            info("Validating page store [node=" + g.cluster().localNode().id() + ", grp=" + grpId + "]");

            CacheGroupContext grp = grid.context().cache().cacheGroup(grpId);

            List<Integer> parts = IntStream.range(0, grp.shared().affinity().affinity(grpId).partitions())
                .boxed().collect(Collectors.toList());

            parts.add(INDEX_PARTITION);

            int realPageSize = grp.dataRegion().pageMemory().realPageSize(grpId);
            int encryptionBlockSize = grp.shared().kernalContext().config().getEncryptionSpi().blockSize();

            for (int p : parts) {
                FilePageStore pageStore =
                    (FilePageStore)((FilePageStoreManager)grp.shared().pageStore()).getStore(grpId, p);

                if (!pageStore.exists())
                    continue;

                String msg = String.format("p=%d, off=%d, total=%d",
                    p, pageStore.encryptedPagesOffset(), pageStore.encryptedPagesCount());

                assertEquals(msg, 0, pageStore.encryptedPagesCount());
                assertEquals(msg, 0, pageStore.encryptedPagesOffset());

                long startPageId = PageIdUtils.pageId(p, PageIdAllocator.FLAG_DATA, 0);

                int pagesCnt = pageStore.pages();
                int pageSize = pageStore.getPageSize();

                ByteBuffer pageBuf = ByteBuffer.allocate(pageSize);

                Path path = new File(pageStore.getFileAbsolutePath()).toPath();

                try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
                    for (int n = 0; n < pagesCnt; n++) {
                        long pageId = startPageId + n;
                        long pageOffset = pageStore.pageOffset(pageId);

                        pageBuf.position(0);

                        ch.position(pageOffset);
                        ch.read(pageBuf);

                        // todo ensure that page is not empty?
                        pageBuf.position(realPageSize + encryptionBlockSize);

                        // If crc present
                        if (pageBuf.getInt() != 0) {
                            msg = String.format("Path=%s, page=%d", pageStore.getFileAbsolutePath(), n);

                            assertEquals(msg, keyId, pageBuf.get() & 0xff);
                        }
                    }
                }
            }
        }
    }

    protected void awaitEncryption(List<Ignite> grids, int grpId, long timeout) throws IgniteCheckedException {
        GridCompoundFuture<Void, ?> fut = new GridCompoundFuture<>();

        for (Ignite node : grids) {
            IgniteEx grid = (IgniteEx)node;

            if (grid.context().clientNode())
                continue;

            IgniteInternalFuture<Void> fut0 = GridTestUtils.runAsync(() -> {
                boolean success =
                    GridTestUtils.waitForCondition(() -> !isReencryptionInProgress(grid, grpId), timeout);

                assertTrue(success);

                return null;
            });

            fut.add(fut0);
        }

        fut.markInitialized();

        fut.get(timeout);
    }

    protected boolean isReencryptionInProgress(IgniteEx node, int grpId) {
        FilePageStoreManager pageStoreMgr = ((FilePageStoreManager)node.context().cache().context().pageStore());

        try {
            for (PageStore pageStore : pageStoreMgr.getStores(grpId)) {
                if (pageStore.encryptedPagesOffset() != pageStore.encryptedPagesCount())
                    return true;
            }
        } catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        return false;
    }
}
