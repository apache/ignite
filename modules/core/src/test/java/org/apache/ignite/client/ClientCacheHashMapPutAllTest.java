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

package org.apache.ignite.client;

import java.util.Map;
import java.util.TreeMap;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class ClientCacheHashMapPutAllTest extends GridCommonAbstractTest {
    /** */
    private static final String HASHMAP_WARN_LSNR_MSG = "Unordered map java.util.LinkedHashMap is used for putAll " +
        "operation on cache default. This can lead to a distributed deadlock";

    /** */
    private static final LogListener lsnr = LogListener.matches(HASHMAP_WARN_LSNR_MSG).times(1).build();

    /** */
    private ListeningTestLogger testLog = new ListeningTestLogger(log);

    /** */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        testLog.registerListener(lsnr);

        cfg.setGridLogger(testLog);

        return cfg;
    }

    /** */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testHashMapPutAll() throws Exception {
        startGrid();

        ClientConfiguration cliCfg = new ClientConfiguration().setAddresses(Config.SERVER);

        try (IgniteClient cln = Ignition.startClient(cliCfg)) {
            ClientCacheConfiguration cacheCfg = new ClientCacheConfiguration()
                .setName(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

            ClientCache<Long, Long> c = cln.createCache(cacheCfg);

            Map<Long, Long> map = new TreeMap<>();

            map.put(0L, 0L);
            map.put(1L, 1L);

            c.putAll(map);
        }

        assertFalse(waitForCondition(lsnr::check, 5_000));
    }
}
