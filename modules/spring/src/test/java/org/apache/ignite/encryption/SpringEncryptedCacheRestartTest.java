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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.encryption.EncryptedCacheRestartTest;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;

/** */
public class SpringEncryptedCacheRestartTest extends EncryptedCacheRestartTest {
    /** {@inheritDoc} */
    @Override protected void createEncCache(IgniteEx grid0, IgniteEx grid1, String cacheName, String cacheGroup) {
        IgniteCache<Long, String> cache = grid0.cache(cacheName());

        for (long i = 0; i < 100; i++)
            cache.put(i, "" + i);
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx[] startTestGrids(boolean cleanPersistenceDir) throws Exception {
        if (cleanPersistenceDir)
            cleanPersistenceDir();

        IgniteEx g0 = (IgniteEx)G.start(
            IgniteUtils.resolveIgnitePath(
                "modules/spring/src/test/config/ignite-config-with-encryption-0.xml").getAbsolutePath());

        IgniteEx g1 = (IgniteEx)G.start(
            IgniteUtils.resolveIgnitePath(
                "modules/spring/src/test/config/ignite-config-with-encryption-1.xml").getAbsolutePath());

        g1.cluster().active(true);

        awaitPartitionMapExchange();

        return new IgniteEx[] {g0, g1};
    }

    /** @throws Exception If failed. */
    public void testThirdNodeJoin() throws Exception {
        IgniteEx[] g = startTestGrids(true);

        IgniteEx g2 = (IgniteEx)G.start(
            IgniteUtils.resolveIgnitePath(
                "modules/spring/src/test/config/ignite-config-with-encryption-3.xml").getAbsolutePath());

        Collection<String> cacheNames = Arrays.asList("encrypted", "encrypted-2");

        for (String cacheName : cacheNames) {
            IgniteInternalCache<Object, Object> enc = g[0].cachex(cacheName);

            assertNotNull(enc);

            int grpId = CU.cacheGroupId(enc.name(), enc.configuration().getGroupName());

            EncryptionKey<?> key0 = g[0].context().encryption().groupKey(grpId);
            EncryptionKey<?> key1 = g[1].context().encryption().groupKey(grpId);
            EncryptionKey<?> key2 = g2.context().encryption().groupKey(grpId);

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
}
