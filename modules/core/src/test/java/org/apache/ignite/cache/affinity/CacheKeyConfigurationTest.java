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

package org.apache.ignite.cache.affinity;

import java.io.Serializable;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests on cache key configuration with different marshallers.
 */
public class CacheKeyConfigurationTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Name of cache used without configured cache key. */
    private static final String CACHE_WO_CACHE_KEY_NAME = "WoCacheKey";

    /** Name of cache used with configured cache key. */
    private static final String CACHE_WITH_CACHE_KEY_NAME = "WithCacheKey";

    /** Name of cache used with key that has field annotated AffinityKeyMapped. */
    private static final String CACHE_WITH_ANNOTATED_KEY_FIELD = "KeyAnnotatedField";

    /** Name of cache used with key that has method annotated AffinityKeyMapped method. */
    private static final String CACHE_WITH_ANNOTATED_KEY_METHOD = "KeyAnnotatedMethod";

    /** Exception message of cache key configuration mismatch in case of non-binary marshaller. */
    private static final String NON_BINARY_MARSH_CONF_MISMATCH_ERROR =
        "Local node's cache keys configuration is not compatible with remote node's cache keys configuration";

    /** Exception message of cache key configuration mismatch in case of binary marshaller. */
    private static final String BINARY_MARSH_CONF_MISMATCH_ERROR = "Binary type has different affinity key fields";

    /** Marshaller. */
    private Marshaller marshaller;

    /** Cache key configurations. */
    private CacheKeyConfiguration[] cacheKeyCfgs;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        CacheConfiguration cacheWoCacheKeyCfg = new CacheConfiguration();
        cacheWoCacheKeyCfg.setName(CACHE_WO_CACHE_KEY_NAME);

        CacheConfiguration cacheWithCacheKeyCfg = new CacheConfiguration();
        cacheWithCacheKeyCfg.setName(CACHE_WITH_CACHE_KEY_NAME);

        CacheConfiguration cacheKeyAnnotatedFieldCfg = new CacheConfiguration();
        cacheKeyAnnotatedFieldCfg.setName(CACHE_WITH_ANNOTATED_KEY_FIELD);

        CacheConfiguration cacheKeyAnnotatedMtdCfg = new CacheConfiguration();
        cacheKeyAnnotatedMtdCfg.setName(CACHE_WITH_ANNOTATED_KEY_METHOD);

        cfg.setCacheConfiguration(cacheWoCacheKeyCfg,
            cacheWithCacheKeyCfg,
            cacheKeyAnnotatedFieldCfg,
            cacheKeyAnnotatedMtdCfg);

        cfg.setMarshaller(marshaller);
        cfg.setCacheKeyConfiguration(cacheKeyCfgs);

        return cfg;
    }

    /**
     * Test cache key configuration with JdkMarshaller.
     *
     * @throws Exception If failed.
     * */
    public void testJdkMarshallerCacheKey() throws Exception {
        marshaller = new JdkMarshaller();
        cacheKeyCfgs = new CacheKeyConfiguration[]{new CacheKeyConfiguration(AffinityKey.class.getName(), "affinityKey")};

        checkCacheKey();
    }

    /**
     * Test cache key configuration with OptimizedMarshaller.
     *
     * @throws Exception If failed.
     * */
    public void testOptimizedMarshallerCacheKey() throws Exception {
        marshaller = new OptimizedMarshaller();
        cacheKeyCfgs = new CacheKeyConfiguration[]{new CacheKeyConfiguration(AffinityKey.class.getName(), "affinityKey")};

        checkCacheKey();
    }

    /**
     * Test cache key configuration with BinaryMarshaller.
     *
     * @throws Exception If failed.
     * */
    public void testBinaryMarshallerCacheKey() throws Exception {
        marshaller = new BinaryMarshaller();
        cacheKeyCfgs = new CacheKeyConfiguration[]{new CacheKeyConfiguration(AffinityKey.class.getName(), "affinityKey")};

        checkCacheKey();
    }

    /**
     * Checks that CacheKeyObject is properly created in accordance with cache and affinity key configurations.
     *
     * @throws Exception If failed.
     * */
    private void checkCacheKey() throws Exception {
        try (Ignite g = startGrid(0)) {
            BaseAffinityKey key1 = new BaseAffinityKey(0, 51);
            AffinityKey key2 = new AffinityKey(0, 51, 100);
            AffinityKeyAnnotatedField key3 = new AffinityKeyAnnotatedField(3, 51, 101);
            AffinityKeyAnnotatedMethod key4 = new AffinityKeyAnnotatedMethod(3, 51, 101);

            int part1 = g.affinity(CACHE_WO_CACHE_KEY_NAME).partition(key1);
            int part2 = g.affinity(CACHE_WITH_CACHE_KEY_NAME).partition(key2);
            int part3 = g.affinity(CACHE_WITH_ANNOTATED_KEY_FIELD).partition(key3);
            int part4 = g.affinity(CACHE_WITH_ANNOTATED_KEY_METHOD).partition(key4);

            assertEquals(51, part1);
            assertEquals(100, part2);
            assertEquals(101, part3);

            if (!(marshaller instanceof BinaryMarshaller))
                assertEquals(101, part4);
        }
    }

    /**
     * Tests that cache key configuration overrides AffinityKeyMapped annotation with JdkMarshaller.
     *
     * @throws Exception If failed.
     * */
    public void testJdkMarshallerCacheKeyOverrides() throws Exception {
        marshaller = new JdkMarshaller();

        cacheKeyCfgs = new CacheKeyConfiguration[] {
                new CacheKeyConfiguration(AffinityKeyAnnotatedField.class.getName(), "baseAffinityKey"),
                new CacheKeyConfiguration(AffinityKeyAnnotatedMethod.class.getName(), "baseAffinityKey"),
        };

        checkCacheKeyOverridesAnnotations();
    }

    /**
     * Tests that cache key configuration overrides AffinityKeyMapped annotation with OptimizedMarshaller.
     *
     * @throws Exception If failed.
     * */
    public void testOptimizedMarshallerCacheKeyOverrides() throws Exception {
        marshaller = new OptimizedMarshaller();

        cacheKeyCfgs = new CacheKeyConfiguration[] {
                new CacheKeyConfiguration(AffinityKeyAnnotatedField.class.getName(), "baseAffinityKey"),
                new CacheKeyConfiguration(AffinityKeyAnnotatedMethod.class.getName(), "baseAffinityKey"),
        };

        checkCacheKeyOverridesAnnotations();
    }


    /**
     * Tests that cache key configuration overrides AffinityKeyMapped annotation with BinaryMarshaller.
     *
     * @throws Exception If failed.
     * */
    public void testBinaryMarshallerCacheKeyOverrides() throws Exception {
        marshaller = new BinaryMarshaller();

        cacheKeyCfgs = new CacheKeyConfiguration[] {
                new CacheKeyConfiguration(AffinityKeyAnnotatedField.class.getName(), "baseAffinityKey"),
                new CacheKeyConfiguration(AffinityKeyAnnotatedMethod.class.getName(), "baseAffinityKey"),
        };

        checkCacheKeyOverridesAnnotations();
    }

    /**
     * Checks that cache key configuration overrides AffinityKeyMapped annotation.
     *
     * @throws Exception If failed.
     * */
    private void checkCacheKeyOverridesAnnotations() throws Exception {
        try (Ignite g = startGrid(0)) {
            AffinityKeyAnnotatedField key1 = new AffinityKeyAnnotatedField(3, 51, 101);
            AffinityKeyAnnotatedMethod key2 = new AffinityKeyAnnotatedMethod(3, 51, 101);

            int part3 = g.affinity(CACHE_WITH_ANNOTATED_KEY_FIELD).partition(key1);
            int part4 = g.affinity(CACHE_WITH_ANNOTATED_KEY_METHOD).partition(key2);

            assertEquals(51, part3);
            assertEquals(51, part4);
        }
    }

    /**
     * Test node startup fails if cache key configuration mismatch - JdkMarshaller.
     *
     * @throws Exception If failed.
     * */
    public void testJdkMarshallerCacheKeyConfigurationMismatch() throws Exception {
        marshaller = new JdkMarshaller();

        checkGridStartupFailedIfCacheKeyConfigurationMismatch();
    }

    /**
     * Test node startup fails if cache key configuration mismatch - OptimizedMarshaller.
     *
     * @throws Exception If failed.
     * */
    public void testJdkOptimizedMarshallerCacheKeyConfigurationMismatch() throws Exception {
        marshaller = new OptimizedMarshaller();

        checkGridStartupFailedIfCacheKeyConfigurationMismatch();
    }

    /**
     * Checks node startup fails if cache key configuration mismatch.
     *
     * @throws Exception If failed.
     * */
    private void checkGridStartupFailedIfCacheKeyConfigurationMismatch() throws Exception {
        IgniteConfiguration cfg1 = getConfiguration(getTestGridName(0));
        cfg1.setCacheKeyConfiguration(
                new CacheKeyConfiguration(AffinityKeyAnnotatedField.class.getName(), "baseAffinityKey"),
                new CacheKeyConfiguration(AffinityKeyAnnotatedMethod.class.getName(), "baseAffinityKey"));

        try (Ignite g1 = startGrid(getTestGridName(0), cfg1)) {
            GridTestUtils.assertThrowsWithCause(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    IgniteConfiguration cfg = getConfiguration(getTestGridName(1));
                    cfg.setCacheKeyConfiguration(
                        new CacheKeyConfiguration(AffinityKeyAnnotatedField.class.getName(), "baseAffinityKey"),
                        new CacheKeyConfiguration(AffinityKeyAnnotatedMethod.class.getName(), "affinityKey"));
                    try (Ignite g2 = startGrid(getTestGridName(1), cfg)) {
                        // No-op.
                    }
                    return null;
                }
            }, IgniteSpiException.class, NON_BINARY_MARSH_CONF_MISMATCH_ERROR);
        }
    }

    /**
     * Test that nodes configured with BinaryMarshaller can't use key class if they have
     * different cache key configurations for it.
     *
     * @throws Exception If failed.
     * */
    public void testBinaryMarshallerCacheKeyConfigurationMismatch() throws Exception {
        marshaller = new BinaryMarshaller();

        checkGridStartupFailedIfCacheKeyConfigurationMismatch();
    }

    /**
     * Affinity key base class.
     * */
    public static class BaseAffinityKey implements Serializable {
        /** */
        private Integer objKey;

        /** */
        private Integer baseAffinityKey;

        /**
         *
         */
        public BaseAffinityKey() {
            // No-op.
        }

        /**
         * @param objKey Key.
         * @param baseAffinityKey Affinity key.
         */
        public BaseAffinityKey(Integer objKey, Integer baseAffinityKey) {
            this.objKey = objKey;
            this.baseAffinityKey = baseAffinityKey;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            BaseAffinityKey that = (BaseAffinityKey) o;

            if (objKey != null ? !objKey.equals(that.objKey) : that.objKey != null) return false;
            return baseAffinityKey != null ? baseAffinityKey.equals(that.baseAffinityKey) : that.baseAffinityKey == null;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = objKey != null ? objKey.hashCode() : 0;
            res = 31 * res + (baseAffinityKey != null ? baseAffinityKey.hashCode() : 0);
            return res;
        }
    }

    /**
     * Affinity key class.
     * */
    public static class AffinityKey extends BaseAffinityKey {
        /** */
        private Integer affinityKey;

        /**
         *
         */
        public AffinityKey() {
            // No-op.
        }

        /**
         * @param objKey Key.
         * @param baseAffinityKey Base affinity key.
         * @param affinityKey Affinity key.
         */
        public AffinityKey(Integer objKey, Integer baseAffinityKey, Integer affinityKey) {
            super(objKey, baseAffinityKey);
            this.affinityKey = affinityKey;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;

            AffinityKey that = (AffinityKey) o;

            return affinityKey != null ? affinityKey.equals(that.affinityKey) : that.affinityKey == null;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = super.hashCode();
            res = 31 * res + (affinityKey != null ? affinityKey.hashCode() : 0);
            return res;
        }
    }

    /**
     * Affinity key class with annotated {@link org.apache.ignite.cache.affinity.AffinityKeyMapped} field.
     * */
    public static class AffinityKeyAnnotatedField extends BaseAffinityKey {
        /** */
        @AffinityKeyMapped
        private Integer affinityKey;

        /**
         *
         */
        public AffinityKeyAnnotatedField() {
            // No-op.
        }

        /**
         * @param objKey Key.
         * @param baseAffinityKey Base affinity key.
         * @param affinityKey Affinity key.
         */
        public AffinityKeyAnnotatedField(Integer objKey, Integer baseAffinityKey, Integer affinityKey) {
            super(objKey, baseAffinityKey);
            this.affinityKey = affinityKey;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;

            AffinityKeyAnnotatedField that = (AffinityKeyAnnotatedField) o;

            return affinityKey != null ? affinityKey.equals(that.affinityKey) : that.affinityKey == null;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = super.hashCode();
            res = 31 * res + (affinityKey != null ? affinityKey.hashCode() : 0);
            return res;
        }
    }

    /**
     * Affinity key class with annotated {@link org.apache.ignite.cache.affinity.AffinityKeyMapped} method.
     * */
    public static class AffinityKeyAnnotatedMethod extends BaseAffinityKey {
        /** */
        private Integer affinityKey;

        /**
         *
         */
        public AffinityKeyAnnotatedMethod() {
            // No-op.
        }

        /**
         * @param objKey Key.
         * @param baseAffinityKey Base affinity key.
         * @param affinityKey Affinity key.
         */
        public AffinityKeyAnnotatedMethod(Integer objKey, Integer baseAffinityKey, Integer affinityKey) {
            super(objKey, baseAffinityKey);
            this.affinityKey = affinityKey;
        }

        /**
         * @return Affinity key.
         */
        @AffinityKeyMapped
        public Integer getAffinityKey() {
            return affinityKey;
        }

        /**
         * @param affinityKey Affinity key.
         */
        public void setAffinityKey(Integer affinityKey) {
            this.affinityKey = affinityKey;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;

            AffinityKeyAnnotatedMethod that = (AffinityKeyAnnotatedMethod) o;

            return affinityKey != null ? affinityKey.equals(that.affinityKey) : that.affinityKey == null;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = super.hashCode();
            res = 31 * res + (affinityKey != null ? affinityKey.hashCode() : 0);
            return res;
        }
    }
}
