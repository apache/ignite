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

import java.io.File;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgnitePdsBinaryMetadataOnClusterRestartTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache1";

    /** */
    private static final String DYNAMIC_TYPE_NAME = "DynamicType";

    /** */
    private static final String DYNAMIC_INT_FIELD_NAME = "intField";

    /** */
    private static final String DYNAMIC_STR_FIELD_NAME = "strField";

    /** */
    private boolean clientMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClientMode(clientMode);

        cfg.setPersistentStoreConfiguration(
            new PersistentStoreConfiguration()
                .setWalMode(WALMode.LOG_ONLY)
        );

        cfg.setCacheConfiguration(new CacheConfiguration()
            .setName(CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction())
            .setCacheMode(CacheMode.REPLICATED));

        return cfg;
    }

    /**
     * Test verifies that binary metadata from regular java classes is saved and restored correctly
     * on cluster restart.
     */
    public void testStaticMetadataIsRestoredOnClusterRestart() throws Exception {
        clientMode = false;

        startGrids(2);

        Ignite ignite0 = grid(0);

        ignite0.active(true);

        IgniteCache<Object, Object> cache0 = ignite0.cache(CACHE_NAME);

        cache0.put(0, new TestValue1(0));
        cache0.put(1, new TestValue2("value"));

        stopAllGrids();

        startGrids(2);

        ignite0 = grid(0);

        ignite0.active(true);

        examineStaticMetadata(2);

        startGrid(2);

        startGrid(3);

        awaitPartitionMapExchange();

        examineStaticMetadata(4);
    }

    /**
     * @param nodesCnt Number of nodes in grid.
     */
    private void examineStaticMetadata(int nodesCnt) {
        for (int i = 0; i < nodesCnt; i++) {
            IgniteCache cache = grid(i).cache(CACHE_NAME).withKeepBinary();

            BinaryObject o1 = (BinaryObject) cache.get(0);

            TestValue1 t1 = o1.deserialize();

            assertEquals(0, t1.getValue());

            BinaryObject o2 = (BinaryObject) cache.get(1);

            TestValue2 t2 = o2.deserialize();

            assertEquals("value", t2.getValue());

            assertEquals(TestValue1.class.getName(), o1.type().typeName());
            assertEquals(TestValue2.class.getName(), o2.type().typeName());
        }
    }

    /**
     * @param nodesCount Nodes count.
     */
    private void examineDynamicMetadata(int nodesCount, BinaryObjectExaminer... examiners) {
        for (int i = 0; i < nodesCount; i++) {
            Ignite ignite = grid(i);

            for (BinaryObjectExaminer examiner : examiners)
                examiner.examine(ignite.cache(CACHE_NAME).withKeepBinary());
        }
    }

    /**
     * Test verifies that metadata for binary types built with BinaryObjectBuilder is saved and updated correctly
     * on cluster restart.
     */
    public void testDynamicMetadataIsRestoredOnClusterRestart() throws Exception {
        clientMode = false;
        //1: start two nodes, add single BinaryObject
        startGrids(2);

        Ignite ignite0 = grid(0);

        ignite0.active(true);

        IgniteCache<Object, Object> cache0 = ignite0.cache(CACHE_NAME);

        BinaryObject bo = ignite0
            .binary()
            .builder(DYNAMIC_TYPE_NAME)
            .setField(DYNAMIC_INT_FIELD_NAME, 10)
            .build();

        cache0.put(2, bo);

        stopAllGrids();

        //2: start two nodes, check BinaryObject added on step 1) (field values, metadata info),
        // modify BinaryObject type, add new instance of modified type
        startGrids(2);

        ignite0 = grid(0);

        ignite0.active(true);

        examineDynamicMetadata(2, examiner0);

        Ignite ignite1 = grid(1);

        BinaryObject bo1 = ignite1
            .binary()
            .builder(DYNAMIC_TYPE_NAME)
            .setField(DYNAMIC_INT_FIELD_NAME, 20)
            .setField(DYNAMIC_STR_FIELD_NAME, "str")
            .build();

        ignite1.cache(CACHE_NAME).put(3, bo1);

        stopAllGrids();

        //3: start two nodes, check both BinaryObject instances,
        // start two additional nodes, check BO instances on all nodes
        startGrids(2);

        ignite0 = grid(0);

        ignite0.active(true);

        examineDynamicMetadata(2, examiner0, examiner1);

        startGrid(2);

        startGrid(3);

        awaitPartitionMapExchange();

        examineDynamicMetadata(4, examiner0, examiner1);
    }

    /**
     * Test verifies that metadata is saved, stored and delivered to client nodes correctly.
     */
    public void testMixedMetadataIsRestoredOnClusterRestart() throws Exception {
        clientMode = false;

        //1: starts 4 nodes one by one and adds java classes and classless BinaryObjects
        // to the same replicated cache from different nodes.
        // Examines all objects in cache (field values and metadata).
        Ignite ignite0 = startGrid(0);

        ignite0.active(true);

        IgniteCache cache0 = ignite0.cache(CACHE_NAME);

        cache0.put(0, new TestValue1(0));

        startGrid(1);

        awaitPartitionMapExchange();

        grid(1).cache(CACHE_NAME).put(1, new TestValue2("value"));

        Ignite ignite2 = startGrid(2);

        awaitPartitionMapExchange();

        BinaryObject bo0 = ignite2.binary().builder(DYNAMIC_TYPE_NAME).setField(DYNAMIC_INT_FIELD_NAME, 10).build();

        ignite2.cache(CACHE_NAME).put(2, bo0);

        Ignite ignite3 = startGrid(3);

        awaitPartitionMapExchange();

        BinaryObject bo1 = ignite3
            .binary()
            .builder(DYNAMIC_TYPE_NAME)
            .setField(DYNAMIC_INT_FIELD_NAME, 20)
            .setField(DYNAMIC_STR_FIELD_NAME, "str")
            .build();

        ignite3.cache(CACHE_NAME).put(3, bo1);

        stopAllGrids();

        startGrids(4);

        grid(0).active(true);

        examineStaticMetadata(4);

        examineDynamicMetadata(4, examiner0, examiner1);

        //2: starts up client node and performs the same set of checks from all nodes including client
        clientMode = true;

        startGrid(4);

        examineStaticMetadata(5);

        examineDynamicMetadata(5, examiner0, examiner1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanIgniteWorkDir();
    }

    /**
     *
     */
    private void cleanIgniteWorkDir() throws IgniteCheckedException {
        String baseDir = U.defaultWorkDirectory();

        File baseDirFile = new File(baseDir);

        for (File f : baseDirFile.listFiles())
            deleteRecursively(U.resolveWorkDirectory(baseDir, f.getName(), false));
    }

    /**
     *
     */
    private static class TestValue1 {
        /** */
        private final int val;

        /**
         * @param val Value.
         */
        TestValue1(int val) {
            this.val = val;
        }

        /**
         *
         */
        int getValue() {
            return val;
        }
    }

    /**
     *
     */
    private static class TestValue2 {
        /** */
        private final String val;

        /**
         * @param val Value.
         */
        TestValue2(String val) {
            this.val = val;
        }

        /**
         *
         */
        String getValue() {
            return val;
        }
    }

    /**
     *
     */
    private static interface BinaryObjectExaminer {
        /**
         * @param cache Cache.
         */
        void examine(IgniteCache cache);
    }

    /** */
    private BinaryObjectExaminer examiner0 = new BinaryObjectExaminer() {
        @Override public void examine(IgniteCache cache) {
            BinaryObject bo = (BinaryObject) cache.get(2);

            assertEquals(DYNAMIC_TYPE_NAME, bo.type().typeName());

            assertEquals(10, bo.field(DYNAMIC_INT_FIELD_NAME));
        }
    };

    /** */
    private BinaryObjectExaminer examiner1 = new BinaryObjectExaminer() {
        @Override public void examine(IgniteCache cache) {
            BinaryObject bo = (BinaryObject) cache.get(3);

            assertEquals(DYNAMIC_TYPE_NAME, bo.type().typeName());

            assertEquals(20, bo.field(DYNAMIC_INT_FIELD_NAME));
            assertEquals("str", bo.field(DYNAMIC_STR_FIELD_NAME));
        }
    };
}
