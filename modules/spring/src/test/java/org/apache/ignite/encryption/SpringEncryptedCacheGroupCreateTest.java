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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** */
public class SpringEncryptedCacheGroupCreateTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(false);

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** @throws Exception If failed. */
    public void testCreateEncryptedCacheGroup() throws Exception {
        IgniteEx g0 = (IgniteEx)G.start(
            IgniteUtils.resolveIgnitePath(
                "modules/spring/src/test/config/ignite-config-with-encryption-2.xml").getAbsolutePath());

        IgniteEx g1 = (IgniteEx)G.start(
            IgniteUtils.resolveIgnitePath(
                "modules/spring/src/test/config/ignite-config-with-encryption-3.xml").getAbsolutePath());

        g1.cluster().active(true);

        awaitPartitionMapExchange();

        IgniteInternalCache<Object, Object> encrypted = g0.cachex("encrypted");

        assertNotNull(encrypted);

        IgniteInternalCache<Object, Object> encrypted2 = g0.cachex("encrypted-2");

        assertNotNull(encrypted2);

        EncryptionKey<?> key = encrypted.context().shared().database().groupKey(
            CU.cacheGroupId(encrypted.name(), encrypted.configuration().getGroupName()));

        assertNotNull(key);
        assertNotNull(key.key());

        EncryptionKey<?> key2 = encrypted2.context().shared().database().groupKey(
            CU.cacheGroupId(encrypted2.name(), encrypted2.configuration().getGroupName()));

        assertNotNull(key2);
        assertNotNull(key2.key());

        assertEquals(key.key(), key2.key());
    }

    /** @throws Exception If failed. */
    public void testCreateNotEncryptedCacheInEncryptedGroupFails() throws Exception {
        IgniteEx g0 = (IgniteEx)G.start(
            IgniteUtils.resolveIgnitePath(
                "modules/spring/src/test/config/ignite-config-with-encryption-4.xml").getAbsolutePath());

        GridTestUtils.assertThrowsWithCause(() -> {
            G.start(IgniteUtils.resolveIgnitePath(
                "modules/spring/src/test/config/ignite-config-with-encryption-5.xml").getAbsolutePath());
        }, IgniteCheckedException.class);
    }
}
