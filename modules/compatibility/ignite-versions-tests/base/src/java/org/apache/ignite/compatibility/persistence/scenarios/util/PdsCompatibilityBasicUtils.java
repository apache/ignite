/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.compatibility.persistence.scenarios.util;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Arrays;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.junit.Assert;

/**
 * Basic utils to modify and check PDS state.
 */
public class PdsCompatibilityBasicUtils {
    /** Test cache name. */
    public static final String TEST_CACHE_NAME = "test";

    /**
     * Provides Ignite configuration for basic pds manipulations.
     *
     * @return Ignite configuration.
     */
    public static IgniteConfiguration configuration() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        CacheConfiguration<Object, Object> cacheCfg = new CacheConfiguration<>();
        cacheCfg.setName(TEST_CACHE_NAME);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cacheCfg.setBackups(0);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        cfg.setCacheConfiguration(cacheCfg);

        cfg.setPersistentStoreConfiguration(
            new PersistentStoreConfiguration().setWalSegmentSize(2 * 1024 * 1024).setWalMode(WALMode.LOG_ONLY)
        );

        return cfg;
    }

    /**
     * Loads various data to ignite using {@link #TEST_CACHE_NAME} cache.
     *
     * @param ignite Ignite instance.
     */
    public static void loadData(Ignite ignite) {
        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(TEST_CACHE_NAME);

        // Write various data with various format.
        for (int i = 0; i < 10; i++)
            cache.put(i + 100500, "data" + i);

        cache.put("1", "2");
        cache.put(12345678, 420);
        cache.put(137421974829014784L, 2356727365723657263L);
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

        // Write 2 megabytes of additional data.
        for (int key = 0; key < 2 * 1024; key++) {
            if (key % 1024 == 0)
                System.out.println("Loading next chunk of data...");

            int[] payload = new int[1024];
            Arrays.fill(payload, key);

            cache.put(key, payload);
        }

        System.out.println("Data loading finished.");
    }

    /**
     * Validates data loaded using {@link #loadData(Ignite)} function.
     *
     * @param ignite Ignite instance.
     */
    public static void validateData(Ignite ignite) {
        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(TEST_CACHE_NAME);

        Assert.assertNotNull(cache);

        for (int i = 0; i < 10; i++)
            Assert.assertEquals("data" + i, cache.get(i + 100500));

        Assert.assertEquals("2", cache.get("1"));
        Assert.assertEquals(420, cache.get(12345678));
        Assert.assertEquals(2356727365723657263L, cache.get(137421974829014784L));
        Assert.assertEquals("Enum_As_Key", cache.get(TestEnum.A));
        Assert.assertEquals(TestEnum.B, cache.get("Enum_As_Value"));
        Assert.assertEquals(TestEnum.C, cache.get(TestEnum.C));
        Assert.assertEquals(new TestSerializable(42), cache.get("Serializable"));
        Assert.assertEquals("Serializable_As_Key", cache.get(new TestSerializable(42)));
        Assert.assertEquals(new TestExternalizable(42), cache.get("Externalizable"));
        Assert.assertEquals("Externalizable_As_Key", cache.get(new TestExternalizable(42)));
        Assert.assertEquals(new TestStringContainerToBePrinted("testStringContainer"), cache.get("testStringContainer"));
        Assert.assertEquals(UUID.fromString("B851B870-3BA7-4E5F-BDB8-458B42300000"),
            cache.get(UUID.fromString("DA9E0049-468C-4680-BF85-D5379164FDCC")));

        // Check 2 megabytes of additional data.
        for (int key = 0; key < 2 * 1024; key++) {
            int[] expPayload = new int[1024];
            Arrays.fill(expPayload, key);

            Assert.assertTrue("Key = " + key, Arrays.equals(expPayload, (int[]) cache.get(key)));
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
            return "TestSerializable [" + "iVal=" + iVal + ']';
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
            return "TestExternalizable[" + "iVal=" + iVal + ']';
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
            return "TestStringContainerToBePrinted[" + "data='" + data + '\'' + ']';
        }
    }
}
