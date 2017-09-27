/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.compatibility.persistence;

import java.io.Serializable;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;

/**
 * Test for new and old style persistent storage folders generation
 */
public class FoldersReuseCompatibiltyTest extends IgnitePersistenceCompatibilityAbstractTest {

    private Serializable configuredConsistentId = null;

    /** Cache name for test. */
    public static final String CACHE_NAME = "dummy";

    public void testFoldersReuseCompatibility() throws Exception {
        startGrid(1, "2.2.0", new ConfigurationClosure(), new PostStartupClosure());

        stopAllGrids();

        IgniteEx ignite = startGrid(0);

        assertEquals(1, ignite.context().discovery().topologyVersion());

        ignite.active(true);

    }

    /** */
    private static class PostStartupClosure implements IgniteInClosure<Ignite> {
        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            ignite.active(true);
            ignite.getOrCreateCache(CACHE_NAME).put("ObjectFromPast", "ValueFromPast");
        }
    }

    /** */
    private class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
           /* cfg.setLocalHost("127.0.0.1");
            TcpDiscoverySpi disco = new TcpDiscoverySpi();
            disco.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);

         cfg.setDiscoverySpi(disco);
*/

            if (configuredConsistentId != null)
                cfg.setConsistentId(configuredConsistentId);
            final PersistentStoreConfiguration psCfg = new PersistentStoreConfiguration();
            cfg.setPersistentStoreConfiguration(psCfg);

            final MemoryConfiguration memCfg = new MemoryConfiguration();
            final MemoryPolicyConfiguration memPolCfg = new MemoryPolicyConfiguration();

            memPolCfg.setMaxSize(32 * 1024 * 1024); // we don't need much memory for this test
            memCfg.setMemoryPolicies(memPolCfg);
            cfg.setMemoryConfiguration(memCfg);


        }
    }
}
