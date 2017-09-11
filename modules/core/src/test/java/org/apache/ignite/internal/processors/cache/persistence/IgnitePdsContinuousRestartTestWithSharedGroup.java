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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.Serializable;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/**
 *
 */
public class IgnitePdsContinuousRestartTestWithSharedGroup extends IgnitePdsContinuousRestartTest {
    /** Ip finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Cache2 name. */
    public static final String CACHE_2_NAME = "cache2";

    /** Cache 2 singleton group name. */
    public static final String CACHE_2_GROUP_NAME = "Group2";

    /** Cache3 name. */
    public static final String CACHE_3_NAME = "cache3";

    /** Cache4 name. */
    public static final String CACHE_4_NAME = "cache4";

    /** */
    public static final String TEST_ATTRIBUTE = "test-attribute";

    /** Dummy grid name. */
    public static final String DUMMY_GRID_NAME = "dummy";

    /** Client grid name. */
    public static final String CLIENT_GRID_NAME = "client";

    /** Daemon grid name. */
    public static final String DAEMON_GRID_NAME = "daemon";

    /**
     * Default constructor.
     */
    public IgnitePdsContinuousRestartTestWithSharedGroup() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoverySpi = (TcpDiscoverySpi)cfg.getDiscoverySpi();
        discoverySpi.setIpFinder(ipFinder);

        BinaryConfiguration bCfg = new BinaryConfiguration();
        bCfg.setCompactFooter(false);

        cfg.setBinaryConfiguration(bCfg);

        CacheConfiguration ccfg1 = new CacheConfiguration();

        ccfg1.setName(CACHE_2_NAME);
        ccfg1.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg1.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg1.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg1.setIndexedTypes(Integer.class, Integer.class);
        ccfg1.setNodeFilter(new TestNodeFilter());

        CacheConfiguration ccfg2 = new CacheConfiguration();

        ccfg2.setName(CACHE_NAME);
        ccfg2.setGroupName(CACHE_2_GROUP_NAME);
        ccfg2.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg2.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg2.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg2.setNodeFilter(new TestNodeFilter());
        ccfg2.setIndexedTypes(Integer.class, TestValue.class);

        CacheConfiguration ccfg3 = new CacheConfiguration();

        ccfg3.setName(CACHE_3_NAME);
        ccfg3.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg3.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg3.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg3.setNodeFilter(new TestNodeFilter());

        CacheConfiguration ccfg4 = new CacheConfiguration()
                .setName(CACHE_4_NAME)
                .setCacheMode(CacheMode.REPLICATED);

        cfg.setCacheConfiguration(ccfg1, ccfg2, ccfg3, ccfg4);

        if (gridName.startsWith(CLIENT_GRID_NAME))
            cfg.setClientMode(true);

        if (DAEMON_GRID_NAME.equals(gridName))
            cfg.setDaemon(true);

        if (gridName.contains(DUMMY_GRID_NAME))
            cfg.setUserAttributes(F.asMap(TEST_ATTRIBUTE, false));
        else
            cfg.setUserAttributes(F.asMap(TEST_ATTRIBUTE, true));

        cfg.setConsistentId(gridName);

        return cfg;
    }



    /**
     *
     */
    private static class TestNodeFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode clusterNode) {
            return Boolean.TRUE.equals(clusterNode.attribute(TEST_ATTRIBUTE));
        }
    }


    /**
     *
     */
    private static class TestValue implements Serializable {
        /** */
        @QuerySqlField(index = true, descending = true)
        private final int v1;

        /** */
        @QuerySqlField(index = true, orderedGroups = @QuerySqlField.Group(name = "idx", order = 1))
        private final int v2;

        @QuerySqlField(index = true, orderedGroups = @QuerySqlField.Group(name = "idx", order = 0))
        private final String v3;

        @QuerySqlField(index = true)
        private final String v4;

        /**
         * @param v1 Value 1.
         * @param v2 Value 2.
         */
        private TestValue(int v1, int v2) {
            this.v1 = v1;
            this.v2 = v2;

            v3 = Integer.toBinaryString(v1) + Integer.toHexString(v2);
            v4 = Integer.toBinaryString(v2) + Integer.toHexString(v1);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestValue value = (TestValue)o;

            return v1 == value.v1 && v2 == value.v2;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = v1;
            result = 31 * result + v2;
            return result;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestValue.class, this);
        }
    }

    /**
     * Check or not topology after grids start
     */
    @Override protected boolean checkTopology() {
        return false;
    }
}
