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

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionKey;
import org.junit.Test;

/** */
public class EncryptedCacheRestartTest extends AbstractEncryptionTest {
    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(false);

        cleanPersistenceDir();
    }

    /** @throws Exception If failed. */
    @Test
    public void testCreateEncryptedCache() throws Exception {
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        createEncryptedCache(grids.get1(), grids.get2(), cacheName(), null);

        checkEncryptedCaches(grids.get1(), grids.get2());

        int grpId = CU.cacheGroupId(cacheName(), null);

        KeystoreEncryptionKey keyBeforeRestart = (KeystoreEncryptionKey)grids.get1().context().encryption().groupKey(grpId);

        stopAllGrids();

        grids = startTestGrids(false);

        checkEncryptedCaches(grids.get1(), grids.get2());

        KeystoreEncryptionKey keyAfterRestart = (KeystoreEncryptionKey)grids.get1().context().encryption().groupKey(grpId);

        assertNotNull(keyAfterRestart);
        assertNotNull(keyAfterRestart.key());

        assertEquals(keyBeforeRestart.key(), keyAfterRestart.key());
    }
}
