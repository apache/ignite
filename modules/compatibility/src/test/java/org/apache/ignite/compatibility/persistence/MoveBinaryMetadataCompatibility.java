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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.junit.Test;

/**
 * Test for moving binary_meta and marshaller folders to PDS.
 */
public class MoveBinaryMetadataCompatibility extends IgnitePersistenceCompatibilityAbstractTest {
    /** Test Ignite version. */
    private static final String IGNITE_VERSION = "2.8.0";

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
                new DataStorageConfiguration()
                        .setDefaultDataRegionConfiguration(
                                new DataRegionConfiguration()
                                        .setPersistenceEnabled(true)
                        ));

        return cfg;
    }

    /**
     * Test that folder migration is successfull
     */
    @Test
    public void test() throws Exception {
        String typeName = "TestBinaryType";

        String fieldName = "testField";

        String fieldVal = "testVal";

        String cacheName = "testCache";

        String objKey = "obj";

        String consid = "node1";

        // start old version and save some binary object to cache
        startGrid(1, IGNITE_VERSION, configuration -> {
            configuration.setConsistentId(consid);
            configuration.setDataStorageConfiguration(
                    new DataStorageConfiguration()
                            .setDefaultDataRegionConfiguration(
                                    new DataRegionConfiguration()
                                            .setPersistenceEnabled(true)
                            ));
        }, (ignite) -> {
            ignite.active(true);

            IgniteBinary binary = ignite.binary();

            BinaryObject test = binary.builder(typeName).setField(fieldName, fieldVal).build();

            IgniteCache<Object, Object> cache = ignite.getOrCreateCache(cacheName).withKeepBinary();

            cache.put(objKey, test);
        });

        stopAllGrids();

        IgniteConfiguration configuration = getConfiguration();

        configuration.setConsistentId(consid);

        Ignite newGrid = IgnitionEx.start(configuration);

        IgniteCache<Object, Object> cache = newGrid.getOrCreateCache(cacheName).withKeepBinary();

        // check that binary object is still available in new version of grid
        BinaryObject obj = (BinaryObject) cache.get(objKey);

        assertTrue(obj.hasField(fieldName));

        assertEquals(fieldVal, obj.field(fieldName));

        newGrid.close();
    }

}
