/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.encryption;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionKey;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 */
public class EncryptedCacheGroupCreateTest extends AbstractEncryptionTest {
    /** */
    public static final String ENCRYPTED_GROUP = "encrypted-group";

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        IgniteEx igniteEx = startGrid(0);

        startGrid(1);

        igniteEx.cluster().active(true);

        awaitPartitionMapExchange();
    }

    /** @throws Exception If failed. */
    @Test
    public void testCreateEncryptedCacheGroup() throws Exception {
        KeystoreEncryptionKey key = createEncryptedCache(ENCRYPTED_CACHE, ENCRYPTED_GROUP);

        CacheConfiguration<Long, String> ccfg = new CacheConfiguration<>(ENCRYPTED_CACHE + "2");

        ccfg.setEncryptionEnabled(true);
        ccfg.setGroupName(ENCRYPTED_GROUP);

        IgniteEx grid = grid(0);

        grid.createCache(ccfg);

        IgniteInternalCache<Object, Object> encrypted2 = grid.cachex(ENCRYPTED_CACHE + "2");

        GridEncryptionManager encMgr = encrypted2.context().kernalContext().encryption();

        KeystoreEncryptionKey key2 = (KeystoreEncryptionKey)encMgr.groupKey(CU.cacheGroupId(ENCRYPTED_CACHE, ENCRYPTED_GROUP));

        assertNotNull(key2);
        assertNotNull(key2.key());

        assertEquals(key.key(), key2.key());
    }

    /** @throws Exception If failed. */
    @Test
    public void testCreateNotEncryptedCacheInEncryptedGroupFails() throws Exception {
        createEncryptedCache(ENCRYPTED_CACHE + "3", ENCRYPTED_GROUP + "3");

        IgniteEx grid = grid(0);

        GridTestUtils.assertThrowsWithCause(() -> {
            grid.createCache(new CacheConfiguration<>(ENCRYPTED_CACHE + "4")
                .setEncryptionEnabled(false)
                .setGroupName(ENCRYPTED_GROUP + "3"));
        }, IgniteCheckedException.class);
    }

    /** */
    private KeystoreEncryptionKey createEncryptedCache(String cacheName, String grpName) {
        CacheConfiguration<Long, String> ccfg = new CacheConfiguration<>(cacheName);

        ccfg.setEncryptionEnabled(true);
        ccfg.setGroupName(grpName);

        IgniteEx grid = grid(0);

        grid.createCache(ccfg);

        IgniteInternalCache<Object, Object> enc = grid.cachex(cacheName);

        assertNotNull(enc);

        KeystoreEncryptionKey key =
            (KeystoreEncryptionKey)grid.context().encryption().groupKey(CU.cacheGroupId(cacheName, grpName));

        assertNotNull(key);
        assertNotNull(key.key());

        return key;
    }
}
