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

import java.util.Collection;
import org.apache.ignite.encryption.EncryptionKey;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.util.typedef.internal.CU;

import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.ENCRYPTION_KEY_PREFIX;

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
    public void testEncryptedCacheDestroy() throws Exception {
        IgniteEx[] grids = startTestGrids(true);

        createEncCache(grids[0], grids[1], cacheName(), null);

        checkEncCaches(grids[0], grids[1]);

        String encryptedCacheName = cacheName();

        grids[0].destroyCache(encryptedCacheName);

        checkCacheDestroyed(grids[1], encryptedCacheName, null, true);

        stopAllGrids(true);

        grids = startTestGrids(false);

        checkCacheDestroyed(grids[0], encryptedCacheName, null, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testEncryptedCacheFromGroupDestroy() throws Exception {
        IgniteEx[] grids = startTestGrids(true);

        String encCacheName = cacheName();

        String grpName = "group1";

        createEncCache(grids[0], grids[1], encCacheName + "2", grpName);
        createEncCache(grids[0], grids[1], encCacheName, grpName);

        checkEncCaches(grids[0], grids[1]);

        grids[0].destroyCache(encCacheName);

        checkCacheDestroyed(grids[1], encCacheName, grpName, false);

        stopAllGrids(true);

        grids = startTestGrids(false);

        checkCacheDestroyed(grids[0], encCacheName, grpName, false);

        grids[0].destroyCache(encCacheName + "2");

        checkCacheDestroyed(grids[0], encCacheName + "2", grpName, true);

        stopAllGrids(true);

        grids = startTestGrids(false);

        checkCacheDestroyed(grids[0], encCacheName, grpName, true);

        checkCacheDestroyed(grids[0], encCacheName + "2", grpName, true);
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

        IgniteCacheDatabaseSharedManager db = grid.context().cache().context().database();

        int grpId = CU.cacheGroupId(encCacheName, grpName);

        EncryptionKey encKey = db.groupKey(grpId);
        MetaStorage metaStore = db.metaStorage();

        if (keyShouldBeEmpty) {
            assertNull(encKey);

            assertNull(metaStore.getData(ENCRYPTION_KEY_PREFIX + grpId));
        } else {
            assertNotNull(encKey);

            assertNotNull(metaStore.getData(ENCRYPTION_KEY_PREFIX + grpId));
        }
    }
}
