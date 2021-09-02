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
package org.apache.ignite.internal.processors.cache.binary;

import java.io.File;
import java.nio.file.Files;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for moving binary metadata and marshaller folders to PDS.
 */
public class BinaryMetadataMoveLegacyFolderTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Before
    @Override public void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @After
    @Override public void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(igniteInstanceName);

        configuration.setConsistentId(UUID.randomUUID());

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        configuration.setDataStorageConfiguration(dsCfg);

        dsCfg.setDefaultDataRegionConfiguration(
            new DataRegionConfiguration()
                .setPersistenceEnabled(true));

        return configuration;
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx startGrid(IgniteConfiguration cfg) throws Exception {
        IgniteEx grid = super.startGrid(cfg);
        grid.active(true);
        return grid;
    }

    /**
     * Get byte representation of simple binary type with one field
     *
     * @param typeName  Type name.
     * @param fieldName Field name.
     */
    private byte[] createBinaryType(String typeName, String fieldName) throws Exception {
        IgniteConfiguration configuration = getConfiguration();

        IgniteEx grid = startGrid(configuration);

        IgniteBinary binary = grid.binary();

        BinaryObjectBuilder builder = binary.builder("TestBinaryType");

        builder.setField(fieldName, 1);

        builder.build();

        BinaryMetadata metadata = ((BinaryTypeImpl)binary.type(typeName)).metadata();

        byte[] marshalled = U.marshal(grid.context(), metadata);

        stopAllGrids();

        cleanPersistenceDir();

        return marshalled;
    }

    /**
     * Test that binary_meta directory, that was previously located in workdir's root, is successfully moving to PDS
     * folder.
     */
    @Test
    public void testBinaryMetadataDirectoryMigration() throws Exception {
        IgniteConfiguration configuration = getConfiguration();

        String typeName = "TestBinaryType";

        String fieldName = "testField";

        // build binary type and get byte representation
        byte[] testBinaryTypeDefinition = createBinaryType(typeName, fieldName);

        File legacyDir = new File(U.resolveWorkDirectory(
            U.defaultWorkDirectory(),
            "binary_meta",
            false
        ), U.maskForFileName(configuration.getConsistentId().toString()));

        legacyDir.mkdirs();

        // just some random type id
        File testBinaryTypeDefFile = new File(legacyDir, "708045005.bin");

        // write binary type definition to legacy folder
        Files.write(testBinaryTypeDefFile.toPath(), testBinaryTypeDefinition);

        IgniteEx grid = startGrid(configuration);

        // legacy metadata dir must be deleted at this moment
        assertFalse(legacyDir.exists());

        IgniteBinary binary = grid.binary();

        // binary type must exist
        BinaryType type = binary.type(typeName);

        assertNotNull(type);

        // it must be the type that we created in the beginning (1 field with specific name)
        Collection<String> strings = type.fieldNames();

        assertEquals(1, strings.size());

        assertTrue(strings.contains(fieldName));

        // try using this binary type to create, put and get object
        BinaryObjectBuilder builder = binary.builder(typeName);

        builder.setField(fieldName, 1);

        IgniteCache<Object, Object> testCache = grid.getOrCreateCache("test").withKeepBinary();

        testCache.put("some", builder.build());

        BinaryObject some = (BinaryObject)testCache.get("some");

        Integer test = some.<Integer>field(fieldName);

        assertEquals(1, test.intValue());
    }



    /**
     * Test that marshaller directory, that was previously located in workdir's root, is successfully moving to PDS
     * folder.
     */
    @Test
    public void testMarshallerMappingsDirectoryMigration() throws Exception {
        IgniteConfiguration configuration = getConfiguration();

        String typeName = "TestBinaryType";

        File legacyDir = U.resolveWorkDirectory(
            U.defaultWorkDirectory(),
            "marshaller",
            false
        );

        // just some random type id
        String typeIdFile = "708045005.classname0";

        File testBinaryTypeDefFile = new File(legacyDir, typeIdFile);

        // write typename definition to legacy folder
        Files.write(testBinaryTypeDefFile.toPath(), typeName.getBytes());

        startGrid(configuration);

        // legacy marshaller mappings dir must be deleted at this moment
        assertFalse(legacyDir.exists());

        File newDir = U.resolveWorkDirectory(
            U.defaultWorkDirectory(),
            DataStorageConfiguration.DFLT_MARSHALLER_PATH,
            false
        );

        // assert folder and contents moved to new location
        assertTrue(newDir.exists());

        assertTrue(new File(newDir, typeIdFile).exists());
    }

}
