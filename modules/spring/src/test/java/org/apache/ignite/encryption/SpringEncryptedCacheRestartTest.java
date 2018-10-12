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

package org.apache.ignite.encryption;

import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.encryption.EncryptedCacheRestartTest;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionKey;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/** */
public class SpringEncryptedCacheRestartTest extends EncryptedCacheRestartTest {
    /** {@inheritDoc} */
    @Override protected void createEncryptedCache(IgniteEx grid0, IgniteEx grid1, String cacheName, String cacheGroup) {
        IgniteCache<Long, String> cache = grid0.cache(cacheName());

        for (long i = 0; i < 100; i++)
            cache.put(i, "" + i);
    }

    /** {@inheritDoc} */
    @Override protected T2<IgniteEx, IgniteEx> startTestGrids(boolean clnPersDir) throws Exception {
        if (clnPersDir)
            cleanPersistenceDir();

        IgniteEx g0 = (IgniteEx)IgnitionEx.start(
            IgniteUtils.resolveIgnitePath(
                "modules/spring/src/test/config/enc/enc-cache.xml").getAbsolutePath(), "grid-0");

        IgniteEx g1 = (IgniteEx)IgnitionEx.start(
            IgniteUtils.resolveIgnitePath(
                "modules/spring/src/test/config/enc/enc-cache.xml").getAbsolutePath(), "grid-1");

        g1.cluster().active(true);

        awaitPartitionMapExchange();

        return new T2<>(g0, g1);
    }

    /** @throws Exception If failed. */
    public void testEncryptionKeysEqualsOnThirdNodeJoin() throws Exception {
        T2<IgniteEx, IgniteEx> g = startTestGrids(true);

        IgniteEx g2 = (IgniteEx)IgnitionEx.start(
            IgniteUtils.resolveIgnitePath(
                "modules/spring/src/test/config/enc/enc-group-2.xml").getAbsolutePath(), "grid-2");

        Collection<String> cacheNames = Arrays.asList("encrypted", "encrypted-2");

        for (String cacheName : cacheNames) {
            IgniteInternalCache<Object, Object> enc = g.get1().cachex(cacheName);

            assertNotNull(enc);

            int grpId = CU.cacheGroupId(enc.name(), enc.configuration().getGroupName());

            KeystoreEncryptionKey key0 = (KeystoreEncryptionKey)g.get1().context().encryption().groupKey(grpId);
            KeystoreEncryptionKey key1 = (KeystoreEncryptionKey)g.get2().context().encryption().groupKey(grpId);
            KeystoreEncryptionKey key2 = (KeystoreEncryptionKey)g2.context().encryption().groupKey(grpId);

            assertNotNull(cacheName, key0);
            assertNotNull(cacheName, key1);
            assertNotNull(cacheName, key2);

            assertNotNull(cacheName, key0.key());
            assertNotNull(cacheName, key1.key());
            assertNotNull(cacheName, key2.key());

            assertEquals(cacheName, key0.key(), key1.key());
            assertEquals(cacheName, key1.key(), key2.key());
        }
    }

    /** @throws Exception If failed. */
    public void testCreateEncryptedCacheGroup() throws Exception {
        IgniteEx g0 = (IgniteEx)IgnitionEx.start(
            IgniteUtils.resolveIgnitePath(
                "modules/spring/src/test/config/enc/enc-group.xml").getAbsolutePath(), "grid-0");

        IgniteEx g1 = (IgniteEx)IgnitionEx.start(
            IgniteUtils.resolveIgnitePath(
                "modules/spring/src/test/config/enc/enc-group-2.xml").getAbsolutePath(), "grid-1");

        g1.cluster().active(true);

        awaitPartitionMapExchange();

        IgniteInternalCache<Object, Object> encrypted = g0.cachex("encrypted");

        assertNotNull(encrypted);

        IgniteInternalCache<Object, Object> encrypted2 = g0.cachex("encrypted-2");

        assertNotNull(encrypted2);

        KeystoreEncryptionKey key = (KeystoreEncryptionKey)g0.context().encryption().groupKey(
            CU.cacheGroupId(encrypted.name(), encrypted.configuration().getGroupName()));

        assertNotNull(key);
        assertNotNull(key.key());

        KeystoreEncryptionKey key2 = (KeystoreEncryptionKey)g0.context().encryption().groupKey(
            CU.cacheGroupId(encrypted2.name(), encrypted2.configuration().getGroupName()));

        assertNotNull(key2);
        assertNotNull(key2.key());

        assertEquals(key.key(), key2.key());
    }

    /** @throws Exception If failed. */
    public void testCreateNotEncryptedCacheInEncryptedGroupFails() throws Exception {
        IgniteEx g0 = (IgniteEx)IgnitionEx.start(
            IgniteUtils.resolveIgnitePath(
                "modules/spring/src/test/config/enc/enc-group.xml").getAbsolutePath(), "grid-0");

        assertThrowsWithCause(() -> {
            try {
                IgnitionEx.start(IgniteUtils.resolveIgnitePath(
                    "modules/spring/src/test/config/enc/not-encrypted-cache-in-group.xml").getAbsolutePath(), "grid-1");
            }
            catch (IgniteCheckedException e) {
                throw new RuntimeException(e);
            }
        }, IgniteCheckedException.class);
    }

    /** @throws Exception If failed. */
    public void testStartWithEncryptedOnDiskPlainInCfg() throws Exception {
        doTestDiffCfgAndPersistentFlagVal(
            "modules/spring/src/test/config/enc/enc-cache.xml",
            "modules/spring/src/test/config/enc/not-encrypted-cache.xml");
    }

    /** @throws Exception If failed. */
    public void testStartWithPlainOnDiskEncryptedInCfg() throws Exception {
        doTestDiffCfgAndPersistentFlagVal(
            "modules/spring/src/test/config/enc/not-encrypted-cache.xml",
            "modules/spring/src/test/config/enc/enc-cache.xml");
    }

    /** */
    private void doTestDiffCfgAndPersistentFlagVal(String cfg1, String cfg2) throws Exception {
        cleanPersistenceDir();

        IgniteEx g = (IgniteEx)IgnitionEx.start(IgniteUtils.resolveIgnitePath(cfg1).getAbsolutePath(), "grid-0");

        g.cluster().active(true);

        IgniteCache c = g.cache("encrypted");

        assertNotNull(c);

        stopAllGrids(false);

        assertThrowsWithCause(() -> {
            try {
                IgnitionEx.start(IgniteUtils.resolveIgnitePath(cfg2).getAbsolutePath(), "grid-0");
            }
            catch (IgniteCheckedException e) {
                throw new RuntimeException(e);
            }
        }, IgniteCheckedException.class);
    }
}
