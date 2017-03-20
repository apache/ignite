/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class GridCacheRebalancingUnmarshallingFailedSelfTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** partitioned cache name. */
    protected static String CACHE = "cache";

    /** Allows to change behavior of readExternal method. */
    protected static AtomicInteger readCnt = new AtomicInteger();

    /** Test key 1. */
    private static class TestKey implements Externalizable {
        /** Field. */
        @QuerySqlField(index = true)
        private String field;

        /**
         * @param field Test key 1.
         */
        public TestKey(String field) {
            this.field = field;
        }

        /** Test key 1. */
        public TestKey() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            TestKey key = (TestKey)o;

            return !(field != null ? !field.equals(key.field) : key.field != null);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return field != null ? field.hashCode() : 0;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(field);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            field = (String)in.readObject();

            if (readCnt.decrementAndGet() <= 0)
                throw new IOException("Class can not be unmarshalled.");
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration iCfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<TestKey, Integer> cfg = new CacheConfiguration<>();

        cfg.setName(CACHE);
        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        cfg.setBackups(0);

        iCfg.setCacheConfiguration(cfg);

        return iCfg;
    }

    /**
     * @throws Exception e.
     */
    public void test() throws Exception {
        String marshClsName = GridTestProperties.getProperty(GridTestProperties.MARSH_CLASS_NAME);

        // This test passes with binary marshaller because we do not unmarshall keys.
        if (marshClsName != null && marshClsName.contains(BinaryMarshaller.class.getSimpleName()))
            return;

        readCnt.set(Integer.MAX_VALUE);

        startGrid(0);

        for (int i = 0; i < 100; i++)
            grid(0).cache(CACHE).put(new TestKey(String.valueOf(i)), i);

        readCnt.set(1);

        startGrid(1);

        readCnt.set(Integer.MAX_VALUE);

        for (int i = 0; i < 50; i++)
            assert grid(1).cache(CACHE).get(new TestKey(String.valueOf(i))) != null;

        stopGrid(0);

        for (int i = 50; i < 100; i++)
            assertNull(grid(1).cache(CACHE).get(new TestKey(String.valueOf(i))));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }
}
