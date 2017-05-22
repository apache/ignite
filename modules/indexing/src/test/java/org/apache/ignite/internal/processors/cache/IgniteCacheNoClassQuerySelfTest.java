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

package org.apache.ignite.internal.processors.cache;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheNoClassQuerySelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    protected Ignite ignite;

    /**
     * @return Atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * @return Distribution.
     */
    protected NearCacheConfiguration nearCacheConfiguration() {
        return new NearCacheConfiguration();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        c.setDiscoverySpi(new TcpDiscoverySpi().setForceServerMode(true).setIpFinder(ipFinder));

        CacheConfiguration cc = defaultCacheConfiguration();

        c.setMarshaller(new BinaryMarshaller());

        cc.setName("cache");

        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setRebalanceMode(SYNC);

        QueryEntity qryEntity = new QueryEntity();

        qryEntity.setKeyType(Integer.class.getName());
        qryEntity.setValueType("MyClass");

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("strField", String.class.getName());
        fields.put("intField", Integer.class.getName());
        fields.put("doubleField", Double.class.getName());

        qryEntity.setFields(fields);

        qryEntity.setFields(fields);

        qryEntity.setIndexes(Arrays.asList(
            new QueryIndex("strField"),
            new QueryIndex("intField"),
            new QueryIndex("doubleField")
        ));

        cc.setQueryEntities(Collections.singletonList(
            qryEntity
        ));

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoClass() throws Exception {
        try {
            startGrid();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("default marshaller"));
        }
    }
}
