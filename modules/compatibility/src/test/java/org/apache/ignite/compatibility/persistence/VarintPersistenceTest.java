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

package org.apache.ignite.compatibility.persistence;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;

/** */
public class VarintPersistenceTest extends IgniteCompatibilityAbstractTest {
    /** */
    private static final String TEST_CACHE_NAME = VarintPersistenceTest.class.getSimpleName();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (!defaultDBWorkDirectoryIsEmpty())
            deleteDefaultDBWorkDirectory();

        assert defaultDBWorkDirectoryIsEmpty() : "DB work directory is not empty.";
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        assert deleteDefaultDBWorkDirectory() : "Couldn't delete DB work directory.";
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        cfg.setPersistentStoreConfiguration(new PersistentStoreConfiguration());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testArraysPersistenceData() throws Exception {
        try {
            startGrid(1, "2.2.0", new PostConfigurationClosure(), new PostActionClosure());
            startGrid(2, "2.2.0", new PostConfigurationClosure());

            stopAllGrids();

            IgniteEx ignite = startGrid(0);
            startGrid(1);

            assertEquals(2, ignite.context().discovery().topologyVersion());

            ignite.active(true);

            IgniteCache<Integer, TestObject> cache = ignite.getOrCreateCache(TEST_CACHE_NAME);

            for (int i = 0; i < 100; i++)
                assertEquals(new TestObject((byte)i), cache.get(i));
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    private static class PostActionClosure implements IgniteInClosure<Ignite> {
        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            ignite.active(true);

            CacheConfiguration<Integer, TestObject> cacheCfg = new CacheConfiguration<>();
            cacheCfg.setName(TEST_CACHE_NAME);
            cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cacheCfg.setBackups(1);
            cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

            IgniteCache<Integer, TestObject> cache = ignite.createCache(cacheCfg);

            for (int i = 0; i < 100; i++)
                cache.put(i, new TestObject((byte)i));
        }
    }

    /** */
    private static class PostConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            cfg.setLocalHost("127.0.0.1");

            TcpDiscoverySpi disco = new TcpDiscoverySpi();
            disco.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);

            cfg.setDiscoverySpi(disco);

            cfg.setPeerClassLoadingEnabled(false);

            cfg.setPersistentStoreConfiguration(new PersistentStoreConfiguration());
        }
    }

    /** */
    private static class TestObject {
        byte[] bArr;
        boolean[] boolArr;
        char[] cArr;
        short[] sArr;
        int[] iArr;
        long[] lArr;
        float[] fArr;
        double[] dArr;
        BigDecimal[] bdArr;
        String[] strArr;
        UUID[] uuidArr;
        Date[] dateArr;
        Timestamp[] tsArr;
        Time[] timeArr;
        TestEnum[] enumArr;
        Object[] objArr;

        /**
         * @param num Number to introduce an entropy.
         */
        public TestObject(byte num) {
            bArr = new byte[] {1, 2, num};
            boolArr = new boolean[] {true, false, true};
            cArr = new char[] {1, 2, (char)num};
            sArr = new short[] {1, 2, num};
            iArr = new int[] {1, 2, num};
            lArr = new long[] {1, 2, num};
            fArr = new float[] {1.1f, 2.2f, num};
            dArr = new double[] {1.1d, 2.2d, num};
            bdArr = new BigDecimal[] {BigDecimal.ZERO, new BigDecimal(1000), new BigDecimal(num)};
            strArr = new String[] {"str1", "str2", String.valueOf(num)};
            uuidArr = new UUID[] {
                UUID.fromString("125E6281-B23A-43C5-B5A4-1CF8861B9C33"),
                UUID.fromString("E9D394D1-7623-47FD-B6ED-F6175CF00000")};
            dateArr = new Date[] {new Date(11111), new Date(22222), new Date(num)};
            tsArr = new Timestamp[] {new Timestamp(11111), new Timestamp(22222), new Timestamp(num)};
            timeArr = new Time[] {new Time(11111), new Time(22222), new Time(num)};
            enumArr = new TestEnum[] {TestEnum.A, TestEnum.B, TestEnum.C};
            objArr = new Object[] {
                UUID.fromString("125E6281-B23A-43C5-B5A4-1CF8861B9C33"),
                UUID.fromString("E9D394D1-7623-47FD-B6ED-F6175CF00000")};
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return GridTestUtils.deepEquals(this, obj);
        }
    }

    /** */
    private enum TestEnum {
        A, B, C
    }
}
