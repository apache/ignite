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
package org.apache.ignite.internal.processors.cache.persistence;

import java.nio.file.Paths;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgnitePdsMarshallerMappingRestoreOnNodeStartTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        int gridIndex = getTestIgniteInstanceIndex(gridName);

        IgniteConfiguration cfg = super.getConfiguration(gridName);

        String tmpDir = System.getProperty("java.io.tmpdir");

        cfg.setWorkDirectory(Paths.get(tmpDir, "srv" + gridIndex).toString());

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
        );

        cfg.setCacheConfiguration(new CacheConfiguration()
            .setName(DEFAULT_CACHE_NAME)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setCacheMode(CacheMode.REPLICATED));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        String tmpDir = System.getProperty("java.io.tmpdir");

        U.delete(Paths.get(tmpDir, "srv0").toFile());
        U.delete(Paths.get(tmpDir, "srv1").toFile());
    }

    /**
     * Test verifies that binary metadata from regular java classes is saved and restored correctly
     * on cluster restart.
     */
    @Test
    public void testStaticMetadataIsRestoredOnRestart() throws Exception {
        startGrids(1);

        Ignite ignite0 = grid(0);

        ignite0.active(true);

        IgniteCache<Object, Object> cache0 = ignite0.cache(DEFAULT_CACHE_NAME);

        cache0.put(0, new TestValue1(0));

        stopAllGrids();

        startGrids(1);

        ignite0 = grid(0);

        ignite0.active(true);

        Ignite ignite1 = startGrid(1);

        awaitPartitionMapExchange();

        ignite1.cache(DEFAULT_CACHE_NAME).get(0);
    }

    /**
     *
     */
    private static class TestValue1 {
        /** */
        @AffinityKeyMapped
        private final int val;

        /**
         * @param val Value.
         */
        TestValue1(int val) {
            this.val = val;
        }

        /** */
        int getValue() {
            return val;
        }
    }
}
