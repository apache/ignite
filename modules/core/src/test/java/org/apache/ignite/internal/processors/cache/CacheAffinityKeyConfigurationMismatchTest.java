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

import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Testing cluster inconsistent affinity configuration.
 * Detect difference property "keyConfiguration" in cache configuration and generate exception.
 */
public class CacheAffinityKeyConfigurationMismatchTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheKeyConfiguration cacheKeyCfg[];

        if (getTestIgniteInstanceName(0).equals(igniteInstanceName)) {
            cacheKeyCfg = new CacheKeyConfiguration[] {
                new CacheKeyConfiguration(AKey.class)
            };
        }
        else if (getTestIgniteInstanceName(1).equals(igniteInstanceName)) {
            cacheKeyCfg = new CacheKeyConfiguration[] {
                getCacheAKeyConfiguration("a")
            };
        }
        else if (getTestIgniteInstanceName(2).equals(igniteInstanceName)) {
            cacheKeyCfg = new CacheKeyConfiguration[] {
                new CacheKeyConfiguration(AKey.class),
                getCacheAKeyConfiguration("a")
            };
        }
        else if (getTestIgniteInstanceName(3).equals(igniteInstanceName)) {
            cacheKeyCfg = new CacheKeyConfiguration[] {
                new CacheKeyConfiguration(AKey.class),
                getCacheAKeyConfiguration("b")
            };
        }
        else if (getTestIgniteInstanceName(4).equals(igniteInstanceName)) {
            cacheKeyCfg = new CacheKeyConfiguration[] {
                getCacheAKeyConfiguration("b")
            };
        }
        else if (getTestIgniteInstanceName(5).equals(igniteInstanceName)) {
            cacheKeyCfg = new CacheKeyConfiguration[] {
            };
        }
        else if (getTestIgniteInstanceName(6).equals(igniteInstanceName)) {
            cacheKeyCfg = new CacheKeyConfiguration[] {
                new CacheKeyConfiguration(AKey.class),
                new CacheKeyConfiguration(BKey.class)
            };
        }
        else if (getTestIgniteInstanceName(7).equals(igniteInstanceName)) {
            cacheKeyCfg = new CacheKeyConfiguration[] {
                new CacheKeyConfiguration(BKey.class)
            };
        }
        else {
            cacheKeyCfg = null;
        }

        if (cacheKeyCfg != null) {
            CacheConfiguration cacheCfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

            cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cacheCfg.setCacheMode(CacheMode.PARTITIONED);
            cacheCfg.setKeyConfiguration(cacheKeyCfg);

            cfg.setCacheConfiguration(cacheCfg);
        }

        return cfg;
    }

    /**
     * Test for matching "keyConfiguration" property.
     *
     * @throws Exception If test fails.
     */
    @Test
    public void testKeyConfigurationMatch() throws Exception {
        try (Ignite ignite0 = startGrid(0)) {
            try (Ignite ignite1 = startGrid(1)) {
                Affinity<Object> affinity0 = ignite0.affinity(DEFAULT_CACHE_NAME);
                Affinity<Object> affinity1 = ignite1.affinity(DEFAULT_CACHE_NAME);

                for (int i = 0; i < Integer.MAX_VALUE; i = i << 1 | 1) {
                    AKey aKey = new AKey(i);

                    assertEquals("different affinity partition for key=" + i,
                        affinity0.partition(aKey),
                        affinity1.partition(aKey)
                    );
                }
            }
        }
    }

    /**
     * Test for checking "keyConfiguration" when property has other field name for a specified name type
     *
     * @throws Exception If test fails.
     */
    @Test
    public void testKeyConfigurationDuplicateTypeName() throws Exception {
        try (Ignite ignite0 = startGrid(2)) {
            try (Ignite ignite1 = startGrid(0)) {
            }
        }

        GridTestUtils.assertThrowsAnyCause(
            log,
            new Callable<Void>() {
                @Override public Void call() throws Exception {
                    try (Ignite ignite = startGrid(3)) {
                    }
                    return null;
                }
            },
            IgniteCheckedException.class, null);

        GridTestUtils.assertThrowsAnyCause(
            log,
            new Callable<Void>() {
                @Override public Void call() throws Exception {
                    try (Ignite ignite0 = startGrid(0)) {
                        try (Ignite ignite1 = startGrid(4)) {
                        }
                    }
                    return null;
                }
            },
            IgniteCheckedException.class, null);
    }

    /**
     * Test when property "keyConfiguration" differs by array size.
     *
     * @throws Exception If test fails.
     */
    @Test
    public void testKeyConfigurationLengthMismatch() throws Exception {
        GridTestUtils.assertThrowsAnyCause(
            log,
            new Callable<Void>() {
                @Override public Void call() throws Exception {
                    try (Ignite ignite0 = startGrid(0)) {
                        try (Ignite ignite1 = startGrid(5)) {
                        }
                    }

                    return null;
                }
            },
            IgniteCheckedException.class,
            "Affinity key configuration mismatch"
        );

        GridTestUtils.assertThrowsAnyCause(
            log,
            new Callable<Void>() {
                @Override public Void call() throws Exception {
                    try (Ignite ignite0 = startGrid(0)) {
                        try (Ignite ignite1 = startGrid(6)) {
                        }
                    }

                    return null;
                }
            },
            IgniteCheckedException.class,
            "Affinity key configuration mismatch"
        );
    }

    /**
     * Test for checking "keyConfiguration" when property has other field name for a specified name type
     *
     * @throws Exception If test fails.
     */
    @Test
    public void testKeyConfigurationMismatch() throws Exception {
        GridTestUtils.assertThrowsAnyCause(
            log,
            new Callable<Void>() {
                @Override public Void call() throws Exception {
                    try (Ignite ignite0 = startGrid(0)) {
                        try (Ignite ignite1 = startGrid(4)) {
                        }
                    }
                    return null;
                }
            },
            IgniteCheckedException.class,
            "Affinity key configuration mismatch"
        );

        GridTestUtils.assertThrowsAnyCause(
            log,
            new Callable<Void>() {
                @Override public Void call() throws Exception {
                    try (Ignite ignite0 = startGrid(0)) {
                        try (Ignite ignite1 = startGrid(7)) {
                        }
                    }
                    return null;
                }
            },
            IgniteCheckedException.class,
            "Affinity key configuration mismatch"
        );
    }

    /**
     * Creating test cache key configuration
     *
     * @param affKeyFieldName Affinity field name.
     * @return Configuration.
     */
    private CacheKeyConfiguration getCacheAKeyConfiguration(String affKeyFieldName) {
        return new CacheKeyConfiguration(AKey.class.getName(), affKeyFieldName);
    }

    /**
     * Value structure for test
     */
    private static class AKey {
        /** */
        @AffinityKeyMapped
        int a;

        public AKey(int a) {
            this.a = a;
        }

        @Override public String toString() {
            return "AKey{a=" + a + '}';
        }
    }

    /**
     * Value structure for test
     */
    private static class BKey {
        /** */
        @AffinityKeyMapped
        int b;

        public BKey(int b) {
            this.b = b;
        }

        @Override public String toString() {
            return "BKey{b=" + b + '}';
        }
    }
}
