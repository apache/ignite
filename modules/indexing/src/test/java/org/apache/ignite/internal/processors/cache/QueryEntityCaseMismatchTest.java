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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.concurrent.Callable;

/**
 * Test reveals issue of null values in SQL query resultset columns that correspond to compound key.
 * That happens when QueryEntity.keyFields has wrong register compared to QueryEntity.fields.
 * Issue only manifests for BinaryMarshaller case. Otherwise the keyFields aren't taken into account.
 */
public class QueryEntityCaseMismatchTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setLocalHost("127.0.0.1");

        cfg.setPeerClassLoadingEnabled(true);

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);
        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:47500..47509"));

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        CacheConfiguration<Object, Integer> ccfg = new CacheConfiguration<>("");

        cfg.setMarshaller(new BinaryMarshaller());

        QueryEntity queryEntity = new QueryEntity("KeyType", Integer.class.getName());

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("a", "TypeA");
        fields.put("b", "TypeB");

        queryEntity.setFields(fields);

        //Specify key fields in upper register
        HashSet<String> keyFields = new HashSet<>();

        keyFields.add("a");
        keyFields.add("B");

        queryEntity.setKeyFields(keyFields);

        ccfg.setQueryEntities(F.asList(queryEntity));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * The cache must not initialize if QueryEntity.keyFields isn't subset of QueryEntity.fields
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testCacheInitializationFailure() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(1);

                return null;
            }
        }, IgniteCheckedException.class, null);
    }
}
