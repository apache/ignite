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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.UUID;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.junit.Test;

/**
 * Saves data using previous version of ignite and then load this data using actual version.
 */
public class PersistenceBasicCompatibilityTest extends IgnitePersistenceCompatibilityAbstractTest {
    /** */
    protected static final String TEST_CACHE_NAME = PersistenceBasicCompatibilityTest.class.getSimpleName();

    /** */
    protected volatile boolean compactFooter;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
                ));

        cfg.setBinaryConfiguration(
            new BinaryConfiguration()
                .setCompactFooter(compactFooter)
        );

        return cfg;
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNodeStartByOldVersionPersistenceData_2_2() throws Exception {
        doTestStartupWithOldVersion("2.2.0");
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNodeStartByOldVersionPersistenceData_2_1() throws Exception {
        doTestStartupWithOldVersion("2.1.0");
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNodeStartByOldVersionPersistenceData_2_3() throws Exception {
        doTestStartupWithOldVersion("2.3.0");
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNodeStartByOldVersionPersistenceData_2_4() throws Exception {
        doTestStartupWithOldVersion("2.4.0");
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNodeStartByOldVersionPersistenceData_2_5() throws Exception {
        doTestStartupWithOldVersion("2.5.0");
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNodeStartByOldVersionPersistenceData_2_6() throws Exception {
        doTestStartupWithOldVersion("2.6.0");
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @param igniteVer 3-digits version of ignite
     * @throws Exception If failed.
     */
    protected void doTestStartupWithOldVersion(String igniteVer, boolean compactFooter) throws Exception {
        boolean prev = this.compactFooter;

        try {
            this.compactFooter = compactFooter;

            startGrid(1, igniteVer, new ConfigurationClosure(compactFooter), new PostStartupClosure());

            stopAllGrids();

            IgniteEx ignite = startGrid(0);

            assertEquals(1, ignite.context().discovery().topologyVersion());

            ignite.active(true);

            validateResultingCacheData(ignite.cache(TEST_CACHE_NAME));
        }
        finally {
            stopAllGrids();

            this.compactFooter = prev;
        }
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @param igniteVer 3-digits version of ignite
     * @throws Exception If failed.
     */
    protected void doTestStartupWithOldVersion(String igniteVer) throws Exception {
        doTestStartupWithOldVersion(igniteVer, true);
    }

    /**
     * @param cache to be filled by different keys and values. Results may be validated in {@link
     * #validateResultingCacheData(Cache)}.
     */
    public static void saveCacheData(Cache<Object, Object> cache) {
        for (int i = 0; i < 10; i++)
            cache.put(i, "data" + i);

        cache.put("1", "2");
        cache.put(12, 2);
        cache.put(13L, 2L);
        cache.put(TestEnum.A, "Enum_As_Key");
        cache.put("Enum_As_Value", TestEnum.B);
        cache.put(TestEnum.C, TestEnum.C);
        cache.put("Serializable", new TestSerializable(42));
        cache.put(new TestSerializable(42), "Serializable_As_Key");
        cache.put("Externalizable", new TestExternalizable(42));
        cache.put(new TestExternalizable(42), "Externalizable_As_Key");
        cache.put("testStringContainer", new TestStringContainerToBePrinted("testStringContainer"));
        cache.put(UUID.fromString("DA9E0049-468C-4680-BF85-D5379164FDCC"),
            UUID.fromString("B851B870-3BA7-4E5F-BDB8-458B42300000"));
    }

    /**
     * Asserts cache contained all expected values as it was saved before.
     *
     * @param cache Cache  should be filled using {@link #saveCacheData(Cache)}.
     */
    public static void validateResultingCacheData(Cache<Object, Object> cache) {
        assertNotNull(cache);

        for (int i = 0; i < 10; i++)
            assertEquals("data" + i, cache.get(i));

        assertEquals("2", cache.get("1"));
        assertEquals(2, cache.get(12));
        assertEquals(2L, cache.get(13L));
        assertEquals("Enum_As_Key", cache.get(TestEnum.A));
        assertEquals(TestEnum.B, cache.get("Enum_As_Value"));
        assertEquals(TestEnum.C, cache.get(TestEnum.C));
        assertEquals(new TestSerializable(42), cache.get("Serializable"));
        assertEquals("Serializable_As_Key", cache.get(new TestSerializable(42)));
        assertEquals(new TestExternalizable(42), cache.get("Externalizable"));
        assertEquals("Externalizable_As_Key", cache.get(new TestExternalizable(42)));
        assertEquals(new TestStringContainerToBePrinted("testStringContainer"), cache.get("testStringContainer"));
        assertEquals(UUID.fromString("B851B870-3BA7-4E5F-BDB8-458B42300000"),
            cache.get(UUID.fromString("DA9E0049-468C-4680-BF85-D5379164FDCC")));
    }

    /** */
    public static class PostStartupClosure implements IgniteInClosure<Ignite> {
        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            ignite.active(true);

            CacheConfiguration<Object, Object> cacheCfg = new CacheConfiguration<>();
            cacheCfg.setName(TEST_CACHE_NAME);
            cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cacheCfg.setBackups(1);
            cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

            IgniteCache<Object, Object> cache = ignite.createCache(cacheCfg);

            saveCacheData(cache);
        }
    }

    /** */
    public static class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** Compact footer. */
        private boolean compactFooter;

        /**
         * @param compactFooter Compact footer.
         */
        public ConfigurationClosure(boolean compactFooter) {
            this.compactFooter = compactFooter;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            cfg.setLocalHost("127.0.0.1");

            TcpDiscoverySpi disco = new TcpDiscoverySpi();
            disco.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);

            cfg.setDiscoverySpi(disco);

            cfg.setPeerClassLoadingEnabled(false);

            cfg.setPersistentStoreConfiguration(new PersistentStoreConfiguration());

            cfg.setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(compactFooter));
        }
    }

    /** Enum for cover binaryObject enum save/load. */
    public enum TestEnum {
        /** */A, /** */B, /** */C
    }

    /** Special class to test WAL reader resistance to Serializable interface. */
    static class TestSerializable implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** I value. */
        private int iVal;

        /**
         * Creates test object
         *
         * @param iVal I value.
         */
        TestSerializable(int iVal) {
            this.iVal = iVal;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestSerializable{" +
                "iVal=" + iVal +
                '}';
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            TestSerializable that = (TestSerializable)o;

            return iVal == that.iVal;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return iVal;
        }
    }

    /** Special class to test WAL reader resistance to Serializable interface. */
    static class TestExternalizable implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** I value. */
        private int iVal;

        /** Noop ctor for unmarshalling */
        public TestExternalizable() {

        }

        /**
         * Creates test object with provided value.
         *
         * @param iVal I value.
         */
        public TestExternalizable(int iVal) {
            this.iVal = iVal;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestExternalizable{" +
                "iVal=" + iVal +
                '}';
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(iVal);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            iVal = in.readInt();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            TestExternalizable that = (TestExternalizable)o;

            return iVal == that.iVal;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return iVal;
        }
    }

    /** Container class to test toString of data records. */
    static class TestStringContainerToBePrinted {
        /** */
        String data;

        /**
         * Creates container.
         *
         * @param data value to be searched in to String.
         */
        public TestStringContainerToBePrinted(String data) {
            this.data = data;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            TestStringContainerToBePrinted printed = (TestStringContainerToBePrinted)o;

            return data != null ? data.equals(printed.data) : printed.data == null;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return data != null ? data.hashCode() : 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestStringContainerToBePrinted{" +
                "data='" + data + '\'' +
                '}';
        }
    }
}
