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

package org.apache.ignite.cdc;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.client.thin.TcpClientCache;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrExpirationInfo;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class TtlClientPutAllConflictTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Ignite ign = startGrid();

        ign.createCache(DEFAULT_CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** */
    @Test
    public void testThinClient() throws Exception {
        try (IgniteClient cln = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1"))) {
            T3<Object, GridCacheVersion, Long> put0 = new T3<>(
                0,
                new GridCacheVersion(1, 1, 1, 2),
                U.currentTimeMillis() + 1000);

            T3<Object, GridCacheVersion, Long> put1 = new T3<>(
                1,
                new GridCacheVersion(1, 1, 2, 2),
                0L);

            Map<Object, T3<Object, GridCacheVersion, Long>> drMap = F.asMap(0, put0, 1, put1);

            TcpClientCache clnCache = (TcpClientCache)cln.cache(DEFAULT_CACHE_NAME);

            clnCache.putAllConflict(drMap);
        }
    }

    /** */
    @Test
    public void testServer() throws Exception {
        IgniteInternalCache cachex = grid().cachex(DEFAULT_CACHE_NAME);

        KeyCacheObject key0 = new KeyCacheObjectImpl(0, null, cachex.affinity().partition(0));
        KeyCacheObject key1 = new KeyCacheObjectImpl(1, null, cachex.affinity().partition(0));

        CacheObject val = new CacheObjectImpl(1, null);
        val.prepareMarshal(cachex.context().cacheObjectContext());

        GridCacheVersion ver0 = new GridCacheVersion(1, 0, 1, 0);
        GridCacheVersion ver1 = new GridCacheVersion(1, 0, 2, 0);

        Map<KeyCacheObject, GridCacheDrInfo> drMap = new HashMap<>();
        drMap.put(key0, new GridCacheDrExpirationInfo(val, ver0, 1_000, U.currentTimeMillis() + 1000));
        drMap.put(key1, new GridCacheDrInfo(val, ver1));

        cachex.putAllConflict(drMap);
    }
}
