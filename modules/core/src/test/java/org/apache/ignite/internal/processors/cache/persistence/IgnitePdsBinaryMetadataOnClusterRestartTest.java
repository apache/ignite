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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

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
    private static final String CUSTOM_WORK_DIR_NAME_PATTERN = "node%s_workDir";

    /** */
    private String customWorkSubDir;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        if (customWorkSubDir != null)
            cfg.setWorkDirectory(Paths.get(U.defaultWorkDirectory(), customWorkSubDir).toString());

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setWalMode(WALMode.LOG_ONLY)
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(100L * 1024 * 1024))
        );

        BinaryConfiguration bCfg = new BinaryConfiguration();

        BinaryTypeConfiguration binaryEnumCfg = new BinaryTypeConfiguration(EnumType.class.getName());

        binaryEnumCfg.setEnum(true);
        binaryEnumCfg.setEnumValues(F.asMap(EnumType.ENUM_VAL_0.name(),
            EnumType.ENUM_VAL_0.ordinal(),
            EnumType.ENUM_VAL_1.name(),
            EnumType.ENUM_VAL_1.ordinal()));

        bCfg.setTypeConfigurations(Arrays.asList(binaryEnumCfg));

        cfg.setBinaryConfiguration(bCfg);

        CacheKeyConfiguration dynamicMetaKeyCfg = new CacheKeyConfiguration(DYNAMIC_TYPE_NAME, DYNAMIC_INT_FIELD_NAME);

        cfg.setCacheKeyConfiguration(dynamicMetaKeyCfg);

        cfg.setCacheConfiguration(new CacheConfiguration()
            .setName(CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction())
            .setCacheMode(CacheMode.REPLICATED));

        return cfg;
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/IGNITE-7258">IGNITE-7258</a> refer to the following JIRA for more context about the problem verified by the test.
     */
    @Test
    public void testUpdatedBinaryMetadataIsPreservedOnJoinToOldCoordinator() throws Exception {
        Ignite ignite0 = startGridInASeparateWorkDir("A");
        Ignite ignite1 = startGridInASeparateWorkDir("B");

        ignite0.active(true);

        IgniteCache<Object, Object> cache0 = ignite0.cache(CACHE_NAME);

        BinaryObject bo = ignite0
            .binary()
            .builder(DYNAMIC_TYPE_NAME)
            .setField(DYNAMIC_INT_FIELD_NAME, 10)
            .build();

        cache0.put(0, bo);

        stopGrid("A");

        IgniteCache<Object, Object> cache1 = ignite1.cache(CACHE_NAME);

        bo = ignite1
            .binary()
            .builder(DYNAMIC_TYPE_NAME)
            .setField(DYNAMIC_INT_FIELD_NAME, 20)
            .setField(DYNAMIC_STR_FIELD_NAME, "str")
            .build();

        cache1.put(1, bo);

        stopAllGrids();

        ignite0 = startGridInASeparateWorkDir("A");
        ignite1 = startGridInASeparateWorkDir("B");

        awaitPartitionMapExchange();

        cache0 = ignite0.cache(CACHE_NAME).withKeepBinary();

        BinaryObject bObj0 = (BinaryObject) cache0.get(0);

        assertEquals(10, (int) bObj0.field("intField"));

        cache1 = ignite1.cache(CACHE_NAME).withKeepBinary();

        BinaryObject bObj1 = (BinaryObject) cache1.get(1);

        assertEquals(20, (int) bObj1.field("intField"));
        assertEquals("str", bObj1.field("strField"));
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/IGNITE-7258">IGNITE-7258</a> refer to the following JIRA for more context about the problem verified by the test.
     */
    @Test
    public void testNewBinaryMetadataIsWrittenOnOldCoordinator() throws Exception {
        Ignite ignite0 = startGridInASeparateWorkDir("A");
        Ignite ignite1 = startGridInASeparateWorkDir("B");

        ignite0.active(true);

        IgniteCache<Object, Object> cache0 = ignite0.cache(CACHE_NAME);

        BinaryObject bObj0 = ignite0.binary()
            .builder("DynamicType0").setField("intField", 10).build();

        cache0.put(0, bObj0);

        stopGrid("A");

        IgniteCache<Object, Object> cache1 = ignite1.cache(CACHE_NAME);

        BinaryObject bObj1 = ignite1.binary()
            .builder("DynamicType1").setField("strField", "str").build();

        cache1.put(1, bObj1);

        stopAllGrids();

        ignite0 = startGridInASeparateWorkDir("A");

        startGridInASeparateWorkDir("B");

        awaitPartitionMapExchange();

        stopGrid("B");

        cache0 = ignite0.cache(CACHE_NAME).withKeepBinary();

        bObj0 = (BinaryObject) cache0.get(0);
        bObj1 = (BinaryObject) cache0.get(1);

        assertEquals("DynamicType0", binaryTypeName(bObj0));
        assertEquals("DynamicType1", binaryTypeName(bObj1));

        assertEquals(10, (int) bObj0.field(DYNAMIC_INT_FIELD_NAME));
        assertEquals("str", bObj1.field(DYNAMIC_STR_FIELD_NAME));
    }

    /**
     * Verifies that newer BinaryMetadata is applied not only on coordinator but on all nodes
     * with outdated version.
     *
     * In the following test nodeB was offline when BinaryMetadata was updated,
     * after full cluster restart it starts second (so it doesn't take the role of coordinator)
     * but metadata update is propagated to it anyway.
     *
     * @see <a href="https://issues.apache.org/jira/browse/IGNITE-7258">IGNITE-7258</a> refer to the following JIRA for more context about the problem verified by the test.
     */
    @Test
    public void testNewBinaryMetadataIsPropagatedToAllOutOfDataNodes() throws Exception {
        Ignite igniteA = startGridInASeparateWorkDir("A");
        startGridInASeparateWorkDir("B");
        Ignite igniteC = startGridInASeparateWorkDir("C");
        startGridInASeparateWorkDir("D");

        igniteA.active(true);

        BinaryObject bObj0 = igniteA.binary()
            .builder(DYNAMIC_TYPE_NAME).setField(DYNAMIC_INT_FIELD_NAME, 10).build();

        igniteA.cache(CACHE_NAME).put(0, bObj0);

        stopGrid("A");
        stopGrid("B");

        BinaryObject bObj1 = igniteC.binary()
            .builder(DYNAMIC_TYPE_NAME)
            .setField(DYNAMIC_INT_FIELD_NAME, 20)
            .setField(DYNAMIC_STR_FIELD_NAME, "str").build();

        igniteC.cache(CACHE_NAME).put(1, bObj1);

        //full cluster restart
        stopAllGrids();

        //node A becomes a coordinator
        igniteA = startGridInASeparateWorkDir("A");
        //node B isn't a coordinator, but metadata update is propagated to if
        startGridInASeparateWorkDir("B");
        //these two nodes will provide an updated version of metadata when started
        startGridInASeparateWorkDir("C");
        startGridInASeparateWorkDir("D");

        awaitPartitionMapExchange();

        //stopping everything to make sure that nodeB will lose its in-memory BinaryMetadata cache
        // and will have to reload it from FS when restarted later
        stopAllGrids();

        Ignite igniteB = startGridInASeparateWorkDir("B");

        igniteB.active(true);

        bObj1 = (BinaryObject) igniteB.cache(CACHE_NAME).withKeepBinary().get(1);

        assertEquals(20, (int) bObj1.field(DYNAMIC_INT_FIELD_NAME));
        assertEquals("str", bObj1.field(DYNAMIC_STR_FIELD_NAME));
    }

    /** */
    private Ignite startGridInASeparateWorkDir(String nodeName) throws Exception {
        customWorkSubDir = String.format(CUSTOM_WORK_DIR_NAME_PATTERN, nodeName);
        return startGrid(nodeName);
    }

    /** */
    private String binaryTypeName(BinaryObject bObj) {
        return bObj.type().typeName();
    }

    /**
     * If joining node has incompatible BinaryMetadata (e.g. when user manually copies binary_meta file),
     * coordinator detects it and fails the node providing information about conflict.
     *
     * @see <a href="https://issues.apache.org/jira/browse/IGNITE-7258">IGNITE-7258</a> refer to the following JIRA for more context about the problem verified by the test.
     */
    @Test
    public void testNodeWithIncompatibleMetadataIsProhibitedToJoinTheCluster() throws Exception {
        final String decimalFieldName = "decField";

        Ignite igniteA = startGridInASeparateWorkDir("A");
        Ignite igniteB = startGridInASeparateWorkDir("B");

        String bConsId = igniteB.cluster().localNode().consistentId().toString();

        igniteA.active(true);

        IgniteCache<Object, Object> cache = igniteA.cache(CACHE_NAME);

        BinaryObject bObj = igniteA.binary()
            .builder(DYNAMIC_TYPE_NAME).setField(decimalFieldName, 10).build();

        cache.put(0, bObj);

        int createdTypeId = igniteA.binary().type(DYNAMIC_TYPE_NAME).typeId();

        stopAllGrids();

        Ignite igniteC = startGridInASeparateWorkDir("C");
        String cConsId = igniteC.cluster().localNode().consistentId().toString();
        igniteC.active(true);

        cache = igniteC.cache(CACHE_NAME);
        bObj = igniteC.binary().builder(DYNAMIC_TYPE_NAME).setField(decimalFieldName, 10L).build();

        cache.put(0, bObj);

        stopAllGrids();

        copyIncompatibleBinaryMetadata(
            String.format(CUSTOM_WORK_DIR_NAME_PATTERN, "C"),
            cConsId,
            String.format(CUSTOM_WORK_DIR_NAME_PATTERN, "B"),
            bConsId,
            DYNAMIC_TYPE_NAME.toLowerCase().hashCode() + ".bin");

        startGridInASeparateWorkDir("A");

        String expectedMsg = String.format(
            "Type '%s' with typeId %d has a different/incorrect type for field '%s'. Expected 'int' but 'long' was " +
                "provided. The type of an existing field can not be changed. Use a different field name or follow " +
                "this procedure to reuse the current name:\n" +
                "- Delete data records that use the old field type;\n" +
                "- Remove metadata by the command: 'control.sh --meta remove --typeId %d'.",
            DYNAMIC_TYPE_NAME,
            createdTypeId,
            decimalFieldName,
            createdTypeId);

        Throwable thrown = GridTestUtils.assertThrows(
            log,
            () -> startGridInASeparateWorkDir("B"),
            Exception.class,
            null);

        assertNotNull(thrown.getCause());
        assertNotNull(thrown.getCause().getCause());

        String actualMsg = thrown.getCause().getCause().getMessage();

        assertTrue("Cause is not correct [expected='" + expectedMsg + "', actual='" + actualMsg + "'].",
            actualMsg.contains(expectedMsg));
    }

    /** */
    private void copyIncompatibleBinaryMetadata(String fromWorkDir,
        String fromConsId,
        String toWorkDir,
        String toConsId,
        String fileName
    ) throws Exception {
        String workDir = U.defaultWorkDirectory();

        Path fromFile = Paths.get(workDir, fromWorkDir, DataStorageConfiguration.DFLT_BINARY_METADATA_PATH,
            fromConsId, fileName);
        Path toFile = Paths.get(workDir, toWorkDir, DataStorageConfiguration.DFLT_BINARY_METADATA_PATH,
            toConsId, fileName);

        Files.copy(fromFile, toFile, StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * Test verifies that binary metadata from regular java classes is saved and restored correctly
     * on cluster restart.
     */
    @Test
    public void testStaticMetadataIsRestoredOnRestart() throws Exception {
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

            assertEquals("val", o1.type().affinityKeyFieldName());
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
    @Test
    public void testDynamicMetadataIsRestoredOnRestart() throws Exception {
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

        examineDynamicMetadata(2, contentExaminer0, structureExaminer0);

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

        examineDynamicMetadata(2, contentExaminer0, contentExaminer1, structureExaminer1);

        startGrid(2);

        startGrid(3);

        awaitPartitionMapExchange();

        examineDynamicMetadata(2, contentExaminer0, contentExaminer1, structureExaminer1);
    }

    /**
     *
     */
    @Test
    public void testBinaryEnumMetadataIsRestoredOnRestart() throws Exception {
        Ignite ignite0 = startGrids(2);

        ignite0.active(true);

        BinaryObject enumBo0 = ignite0.binary().buildEnum(EnumType.class.getName(), 0);

        ignite0.cache(CACHE_NAME).put(4, enumBo0);

        stopAllGrids();

        ignite0 = startGrids(2);

        ignite0.active(true);

        startGrid(2);

        awaitPartitionMapExchange();

        examineDynamicMetadata(3, enumExaminer0);

        stopAllGrids();

        ignite0 = startGrids(3);

        ignite0.active(true);

        startClientGrid(3);

        awaitPartitionMapExchange();

        examineDynamicMetadata(4, enumExaminer0);
    }

    /**
     * Test verifies that metadata is saved, stored and delivered to client nodes correctly.
     */
    @Test
    public void testMixedMetadataIsRestoredOnRestart() throws Exception {
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

        examineDynamicMetadata(4, contentExaminer0, contentExaminer1, structureExaminer1);

        //2: starts up client node and performs the same set of checks from all nodes including client
        startClientGrid(4);

        examineStaticMetadata(5);

        examineDynamicMetadata(5, contentExaminer0, contentExaminer1, structureExaminer1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanIgniteWorkDir();

        customWorkSubDir = null;
    }

    /**
     *
     */
    private void cleanIgniteWorkDir() throws IgniteCheckedException {
        String baseDir = U.defaultWorkDirectory();

        File baseDirFile = new File(baseDir);

        for (File f : baseDirFile.listFiles())
            U.delete(U.resolveWorkDirectory(baseDir, f.getName(), false));
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

    /** */
    private enum EnumType {
        /** */ ENUM_VAL_0,
        /** */ ENUM_VAL_1
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
    private BinaryObjectExaminer contentExaminer0 = new BinaryObjectExaminer() {
        @Override public void examine(IgniteCache cache) {
            BinaryObject bo = (BinaryObject) cache.get(2);

            int fieldVal = bo.field(DYNAMIC_INT_FIELD_NAME);

            assertEquals(10, fieldVal);
        }
    };

    /** */
    private BinaryObjectExaminer contentExaminer1 = new BinaryObjectExaminer() {
        @Override public void examine(IgniteCache cache) {
            BinaryObject bo = (BinaryObject) cache.get(3);

            int fieldVal = bo.field(DYNAMIC_INT_FIELD_NAME);

            assertEquals(20, fieldVal);
            assertEquals("str", bo.field(DYNAMIC_STR_FIELD_NAME));
        }
    };

    /** */
    private BinaryObjectExaminer structureExaminer0 = new BinaryObjectExaminer() {
        @Override public void examine(IgniteCache cache) {
            BinaryObject bo = (BinaryObject) cache.get(2);

            BinaryType type = bo.type();

            assertFalse(type.isEnum());

            assertEquals(DYNAMIC_TYPE_NAME, type.typeName());

            Collection<String> fieldNames = type.fieldNames();

            assertEquals(1, fieldNames.size());

            assertTrue(fieldNames.contains(DYNAMIC_INT_FIELD_NAME));

            assertEquals(DYNAMIC_INT_FIELD_NAME, type.affinityKeyFieldName());
        }
    };

    /** */
    private BinaryObjectExaminer structureExaminer1 = new BinaryObjectExaminer() {
        @Override public void examine(IgniteCache cache) {
            BinaryObject bo = (BinaryObject) cache.get(2);

            BinaryType type = bo.type();

            assertFalse(bo.type().isEnum());

            assertEquals(DYNAMIC_TYPE_NAME, type.typeName());

            Collection<String> fieldNames = type.fieldNames();

            assertEquals(2, fieldNames.size());

            assertTrue(fieldNames.contains(DYNAMIC_INT_FIELD_NAME));
            assertTrue(fieldNames.contains(DYNAMIC_STR_FIELD_NAME));
        }
    };

    /** */
    private BinaryObjectExaminer enumExaminer0 = new BinaryObjectExaminer() {
        @Override public void examine(IgniteCache cache) {
            BinaryObject enumBo = (BinaryObject) cache.get(4);

            assertEquals(EnumType.ENUM_VAL_0.ordinal(), enumBo.enumOrdinal());

            BinaryType type = enumBo.type();

            assertTrue(type.isEnum());

            assertEquals(EnumType.class.getName(), type.typeName());

            Collection<BinaryObject> enumVals = type.enumValues();

            assertEquals(2, enumVals.size());

            int i = 0;

            for (BinaryObject bo : enumVals) {
                assertEquals(i, bo.enumOrdinal());

                assertEquals("ENUM_VAL_" + (i++), bo.enumName());
            }
        }
    };
}
