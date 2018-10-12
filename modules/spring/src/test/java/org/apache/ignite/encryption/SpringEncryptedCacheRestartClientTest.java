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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.encryption.EncryptedCacheRestartTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.T2;

/** */
public class SpringEncryptedCacheRestartClientTest extends EncryptedCacheRestartTest {
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

        IgniteEx client = (IgniteEx)IgnitionEx.start(
            IgniteUtils.resolveIgnitePath(
                "modules/spring/src/test/config/enc/enc-cache-client.xml").getAbsolutePath(), "client");

        g1.cluster().active(true);

        awaitPartitionMapExchange();

        return new T2<>(g1, client);
    }
}
