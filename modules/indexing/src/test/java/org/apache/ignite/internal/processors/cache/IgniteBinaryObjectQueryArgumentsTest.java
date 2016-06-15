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
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteBinaryObjectQueryArgumentsTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        CacheConfiguration ccfg = new CacheConfiguration();
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        QueryEntity person = new QueryEntity();
        person.setKeyType(TestKey.class.getName());
        person.setValueType(Person.class.getName());
        person.addQueryField("name", String.class.getName(), null);

        ccfg.setQueryEntities(Arrays.asList(person));

        cfg.setCacheConfiguration(ccfg);

        cfg.setMarshaller(null);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testObjectArgument() throws Exception {
        IgniteCache<TestKey, Person> cache = ignite(0).cache(null);

        for (int i = 0; i < 100; i++)
            cache.put(new TestKey(i), new Person("name-" + i));

        SqlQuery<TestKey, Person> qry = new SqlQuery<>(Person.class, "where _key=?");

        IgniteBinary binary = ignite(0).binary();

        for (int i = 0; i < 100; i++) {
            Object key = new TestKey(i);

            if (i % 2 == 0)
                key = binary.toBinary(key);

            qry.setArgs(key);

            List<Cache.Entry<TestKey, Person>> res = cache.query(qry).getAll();

            assertEquals(1, res.size());

            Person p = res.get(0).getValue();

            assertEquals("name-" + i, p.name);
        }
    }

    /**
     *
     */
    private static class Person {
        /** */
        String name;

        /**
         * @param name Name.
         */
        public Person(String name) {
            this.name = name;
        }
    }

    /**
     *
     */
    public static class TestKey {
        /** */
        private int id;

        /**
         * @param id Key.
         */
        public TestKey(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestKey other = (TestKey)o;

            return id == other.id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }
}
