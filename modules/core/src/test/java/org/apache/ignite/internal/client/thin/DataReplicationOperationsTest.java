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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.Person;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.platform.cache.expiry.PlatformExpiryPolicy;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.testframework.GridTestUtils.cartesianProduct;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Data replication operations test.
 */
@RunWith(Parameterized.class)
public class DataReplicationOperationsTest extends AbstractThinClientTest {
    /** Keys count. */
    private static final int KEYS_CNT = 10;

    /** */
    private static IgniteClient client;

    /** */
    private static TcpClientCache<Object, Object> cache;

    /** */
    private final GridCacheVersion otherVer = new GridCacheVersion(1, 1, 1, 2);

    /** {@code True} if operate with binary objects. */
    @Parameterized.Parameter
    public boolean binary;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "binary={0}")
    public static Collection<Object[]> parameters() {
        return cartesianProduct(F.asList(false, true));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid();

        client = startClient(grid());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        grid().destroyCaches(grid().cacheNames());

        cache = (TcpClientCache<Object, Object>)client.createCache(DEFAULT_CACHE_NAME);

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
        Map<Object, T2<Object, GridCacheVersion>> data = createPutAllData();

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
    public void testWithExpiryPolicy() throws Exception {
        PlatformExpiryPolicy expPlc = new PlatformExpiryPolicy(1000, 1000, 1000);

        ClientCacheConfiguration ccfgWithExpPlc = new ClientCacheConfiguration()
            .setName("cache-with-expiry-policy")
            .setExpiryPolicy(expPlc);

        TcpClientCache<Object, Object> cache = (TcpClientCache<Object, Object>)client.getOrCreateCache(ccfgWithExpPlc);

        TcpClientCache<Object, Object> cacheWithExpPlc = binary ?
            (TcpClientCache<Object, Object>)cache.withKeepBinary() : cache;

        Map<Object, T2<Object, GridCacheVersion>> data = createPutAllData();

        cacheWithExpPlc.putAllConflict(data);

        assertTrue(cacheWithExpPlc.containsKeys(data.keySet()));

        assertTrue(waitForCondition(
            () -> data.keySet().stream().noneMatch(cacheWithExpPlc::containsKey),
            2 * expPlc.getExpiryForCreation().getDurationAmount()
        ));
    }

    /** */
    private Map<Object, T2<Object, GridCacheVersion>> createPutAllData() {
        Map<Object, T2<Object, GridCacheVersion>> map = new HashMap<>();

        for (int i = 0; i < KEYS_CNT; i++) {
            Person key = new Person(i, "Person-" + i);
            Person val = new Person(i, "Person-" + i);

            map.put(binary ? client.binary().toBinary(key) : key,
                new T2<>(binary ? client.binary().toBinary(val) : val, otherVer));
        }

        return map;
    }
}
