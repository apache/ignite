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

package org.apache.ignite.compatibility.binary;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Tests of storing data in compatibility mode.
 */
public class VarintArraysSizeCompatibilityTest extends IgniteCompatibilityAbstractTest {
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();
        cfg.setPeerClassLoadingEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(IgniteSystemProperties.IGNITE_NO_VARINT_ARRAY_LENGTH, "true");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.clearProperty(IgniteSystemProperties.IGNITE_NO_VARINT_ARRAY_LENGTH);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /**
     * @throws Exception If failed.
     */
    public void testArraysStoringInCompatibilityMode() throws Exception {
        startGrid(1, "2.1.0", new PostConfigurationClosure());

        startGrid(2, "2.1.0", new PostConfigurationClosure());

        Ignite ignite = startGrid(0);

        CacheConfiguration<String, TestObject> cacheCfg = defaultCacheConfiguration();
        cacheCfg.setCacheMode(CacheMode.REPLICATED);
        cacheCfg.setName("varintTestCache");
        cacheCfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        IgniteCache<String, TestObject> cache = ignite.createCache(cacheCfg);

        final TestObject obj = new TestObject();

        final String key = "key";

        cache.put(key, obj);

        cache.invoke(
            key,
            new EntryProcessor<String, TestObject, TestObject>() {
                @Override public TestObject process(MutableEntry<String, TestObject> entry,
                    Object... objects) throws EntryProcessorException {
                    TestObject val = entry.getValue();

                    assertEquals(obj, val);

                    return val;
                }
            });
    }

    /** */
    private static class TestObject {
        byte[] bArr = new byte[] {1, 2, 3};
        boolean[] boolArr = new boolean[] {true, false, true};
        char[] cArr = new char[] {1, 2, 3};
        short[] sArr = new short[] {1, 2, 3};
        int[] iArr = new int[] {1, 2, 3};
        long[] lArr = new long[] {1, 2, 3};
        float[] fArr = new float[] {1.1f, 2.2f, 3.3f};
        double[] dArr = new double[] {1.1d, 2.2d, 3.3d};
        BigDecimal[] bdArr = new BigDecimal[] {BigDecimal.ZERO, new BigDecimal(1000), new BigDecimal(123456789)};
        String[] strArr = new String[] {"str1", "str2", "str3"};
        UUID[] uuidArr = new UUID[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        Date[] dateArr = new Date[] {new Date(11111), new Date(22222), new Date(33333)};
        Timestamp[] tsArr = new Timestamp[] {new Timestamp(11111), new Timestamp(22222), new Timestamp(33333)};
        Time[] timeArr = new Time[] {new Time(11111), new Time(22222), new Time(33333)};
        TestEnum[] enumArr = new TestEnum[] {TestEnum.A, TestEnum.B, TestEnum.C};
        Object[] objArr = new Object[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = Arrays.hashCode(bArr);
            result = 31 * result + Arrays.hashCode(boolArr);
            result = 31 * result + Arrays.hashCode(cArr);
            result = 31 * result + Arrays.hashCode(sArr);
            result = 31 * result + Arrays.hashCode(iArr);
            result = 31 * result + Arrays.hashCode(lArr);
            result = 31 * result + Arrays.hashCode(fArr);
            result = 31 * result + Arrays.hashCode(dArr);
            result = 31 * result + Arrays.hashCode(bdArr);
            result = 31 * result + Arrays.hashCode(strArr);
            result = 31 * result + Arrays.hashCode(uuidArr);
            result = 31 * result + Arrays.hashCode(dateArr);
            result = 31 * result + Arrays.hashCode(tsArr);
            result = 31 * result + Arrays.hashCode(timeArr);
            result = 31 * result + Arrays.hashCode(enumArr);
            result = 31 * result + Arrays.hashCode(objArr);
            return result;
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

    /** */
    private static class PostConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            cfg.setLocalHost("127.0.0.1");

            TcpDiscoverySpi disco = new TcpDiscoverySpi();
            disco.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);

            cfg.setDiscoverySpi(disco);

            cfg.setPeerClassLoadingEnabled(true);
        }
    }
}