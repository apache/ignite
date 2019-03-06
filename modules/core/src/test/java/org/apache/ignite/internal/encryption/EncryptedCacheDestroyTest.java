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

import java.util.Collection;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionKey;
import org.junit.Test;

import static org.apache.ignite.internal.managers.encryption.GridEncryptionManager.ENCRYPTION_KEY_PREFIX;

/**
 */
public class EncryptedCacheDestroyTest extends AbstractEncryptionTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEncryptedCacheDestroy() throws Exception {
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        createEncryptedCache(grids.get1(), grids.get2(), cacheName(), null);

        checkEncryptedCaches(grids.get1(), grids.get2());

        String encryptedCacheName = cacheName();

        grids.get1().destroyCache(encryptedCacheName);

        checkCacheDestroyed(grids.get2(), encryptedCacheName, null, true);

        stopAllGrids(true);

        grids = startTestGrids(false);

        checkCacheDestroyed(grids.get1(), encryptedCacheName, null, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEncryptedCacheFromGroupDestroy() throws Exception {
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        String encCacheName = cacheName();

        String grpName = "group1";

        createEncryptedCache(grids.get1(), grids.get2(), encCacheName + "2", grpName);
        createEncryptedCache(grids.get1(), grids.get2(), encCacheName, grpName);

        checkEncryptedCaches(grids.get1(), grids.get2());

        grids.get1().destroyCache(encCacheName);

        checkCacheDestroyed(grids.get2(), encCacheName, grpName, false);

        stopAllGrids(true);

        grids = startTestGrids(false);

        checkCacheDestroyed(grids.get1(), encCacheName, grpName, false);

        grids.get1().destroyCache(encCacheName + "2");

        checkCacheDestroyed(grids.get1(), encCacheName + "2", grpName, true);

        stopAllGrids(true);

        grids = startTestGrids(false);

        checkCacheDestroyed(grids.get1(), encCacheName, grpName, true);

        checkCacheDestroyed(grids.get1(), encCacheName + "2", grpName, true);
    }

    /** */
    private void checkCacheDestroyed(IgniteEx grid, String encCacheName, String grpName, boolean keyShouldBeEmpty)
        throws Exception {
        awaitPartitionMapExchange();

        Collection<String> cacheNames = grid.cacheNames();

        for (String cacheName : cacheNames) {
            if (cacheName.equals(encCacheName))
                fail(encCacheName + " should be destroyed.");
        }

        int grpId = CU.cacheGroupId(encCacheName, grpName);

        KeystoreEncryptionKey encKey = (KeystoreEncryptionKey)grid.context().encryption().groupKey(grpId);
        MetaStorage metaStore = grid.context().cache().context().database().metaStorage();

        if (keyShouldBeEmpty) {
            assertNull(encKey);

            assertNull(metaStore.readRaw(ENCRYPTION_KEY_PREFIX + grpId));
        } else {
            assertNotNull(encKey);

            assertNotNull(metaStore.readRaw(ENCRYPTION_KEY_PREFIX + grpId));
        }
    }
}
