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

package org.apache.ignite.internal.client.thin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.Person;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Data replication operations test.
 */
@RunWith(Parameterized.class)
public class DataReplicationOperationsTest extends AbstractThinClientTest {
    /** Keys count. */
    private static final int KEYS_CNT = 10;

    /** TTL. */
    public static final int TTL = 1000;

    /** */
    private static IgniteClient client;

    /** */
    private static TcpClientCache<Object, Object> cache;

    /** */
    private final GridCacheVersion otherVer = new GridCacheVersion(1, 1, 1, 2);

    /** {@code True} if operate with binary objects. */
    @Parameterized.Parameter
    public boolean binary;

    /** Cache mode. */
    @Parameterized.Parameter(1)
    public CacheAtomicityMode mode;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "binary={0}, cacheMode={1}")
    public static Collection<Object[]> parameters() {
        List<Object[]> params = new ArrayList<>();

        for (boolean binary : new boolean[]{false, true})
            for (CacheAtomicityMode mode : new CacheAtomicityMode[]{TRANSACTIONAL, ATOMIC})
                params.add(new Object[]{binary, mode});

        return params;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);

        client = startClient(grid(0));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        grid(0).destroyCaches(grid(0).cacheNames());

        ClientCacheConfiguration ccfg = new ClientCacheConfiguration()
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(mode);

        cache = (TcpClientCache<Object, Object>)client.createCache(ccfg);

        if (binary)
            cache = (TcpClientCache<Object, Object>)cache.withKeepBinary();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        client.close();
    }

    /** */
    @Test
    public void testPutAllConflict() {
        Map<Object, T3<Object, GridCacheVersion, Long>> data = createPutAllData(CU.EXPIRE_TIME_ETERNAL);

        cache.putAllConflict(data);

        data.forEach((key, val) -> assertEquals(val.get1(), cache.get(key)));
    }

    /** */
    @Test
    public void testRemoveAllConflict() {
        for (int i = 0; i < KEYS_CNT; i++)
            cache.put(new Person(i, "Person-" + i), new Person(i, "Person-" + i));

        Map<Object, GridCacheVersion> map = new HashMap<>();

        for (int i = 0; i < KEYS_CNT; i++) {
            Person key = new Person(i, "Person-" + i);

            map.put(binary ? client.binary().toBinary(key) : key, otherVer);
        }

        cache.removeAllConflict(map);

        map.keySet().forEach(key -> assertFalse(cache.containsKey(key)));
    }

    /** @throws Exception If fails. */
    @Test
    public void testWithPerEntryExpiry() throws Exception {
        TcpClientCache<Object, Object> cache0 =
            (TcpClientCache<Object, Object>)client.getOrCreateCache(DEFAULT_CACHE_NAME);

        TcpClientCache<Object, Object> cache = binary ?
            (TcpClientCache<Object, Object>)cache0.withKeepBinary() : cache0;

        Map<Object, T3<Object, GridCacheVersion, Long>> data = createPutAllData(System.currentTimeMillis() + TTL);

        cache.putAllConflict(data);

        assertTrue(cache.containsKeys(data.keySet()));

        assertTrue(waitForCondition(() -> data.keySet().stream().noneMatch(cache::containsKey), 2 * TTL));
    }

    /** */
    @Test
    public void testMixedExpiryTime() {
        Map<Object, T3<Object, GridCacheVersion, Long>> put0 = createPutAllData(System.currentTimeMillis() + TTL);
        Map<Object, T3<Object, GridCacheVersion, Long>> put1 = createPutAllData(KEYS_CNT, CU.EXPIRE_TIME_ETERNAL);

        Map<Object, T3<Object, GridCacheVersion, Long>> drMap = new HashMap<>();
        drMap.putAll(put0);
        drMap.putAll(put1);

        cache.putAllConflict(drMap);

        assertTrue(cache.containsKeys(put0.keySet()));
        assertTrue(cache.containsKeys(put1.keySet()));
    }

    /** */
    private Map<Object, T3<Object, GridCacheVersion, Long>> createPutAllData(long expireTime) {
        return createPutAllData(0, expireTime);
    }

    /** */
    private Map<Object, T3<Object, GridCacheVersion, Long>> createPutAllData(int startKey, long expireTime) {
        Map<Object, T3<Object, GridCacheVersion, Long>> map = new HashMap<>();

        for (int i = startKey; i < startKey + KEYS_CNT; i++) {
            Person key = new Person(i, "Person-" + i);
            Person val = new Person(i, "Person-" + i);

            map.put(binary ? client.binary().toBinary(key) : key,
                new T3<>(binary ? client.binary().toBinary(val) : val, otherVer, expireTime));
        }

        return map;
    }
}
