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

package org.apache.ignite.cdc;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAIT_SCHEMA_UPDATE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Check async update of metadata.
 */
public class MetadataUpdateTest extends AbstractCdcTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)))
            .setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(true));
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_WAIT_SCHEMA_UPDATE, value = "500")
    public void testAsyncBinaryMetaUpdate() throws Exception {
        byte[] bytes;
        BinaryMetadata btype;

        try (IgniteEx ignite = startGrid(0)) {
            ignite.cluster().state(ACTIVE);

            BinaryObjectBuilder bldr = ignite.binary().builder("MyType");

            bldr.setField("one", "two");
            bldr.setField("number", 2);

            // Build & serialize binary object.
            bytes = IgniteUtils.toBytes(bldr.build());

            // Get type description.
            btype = ((BinaryTypeImpl)ignite.binary().type("MyType")).metadata();

            assertNotNull(btype);
        }

        // Cleaning metadata.
        cleanPersistenceDir();

        try (IgniteEx ignite = startGrid(0)) {
            ignite.cluster().state(ACTIVE);

            IgniteCache<BinaryObject, BinaryObject> cache =
                ignite.getOrCreateCache(DEFAULT_CACHE_NAME).withKeepBinary();

            IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

            BinaryObject bobj = IgniteUtils.fromBytes(bytes);

            // Schema not analyzed during cache operations.
            cache.put(bobj, bobj);
            BinaryObject bobj2 = cache.get(bobj);

            // BinarType(schema) unknown, query of field fail.
            assertThrowsWithCause(() -> bobj2.field("one"), BinaryObjectException.class);
            assertThrowsWithCause(() -> bobj2.field("number"), BinaryObjectException.class);
            assertThrowsWithCause(() -> bobj.field("one"), BinaryObjectException.class);
            assertThrowsWithCause(() -> bobj.field("number"), BinaryObjectException.class);

            ignite.context().cacheObjects().addMeta(
                btype.typeId(),
                new BinaryTypeImpl(
                    ((CacheObjectBinaryProcessorImpl)ignite.context().cacheObjects()).binaryContext(),
                    btype
                ),
                false
            );

            assertEquals("two", bobj.field("one"));
            assertEquals("two", bobj2.field("one"));
            assertEquals((Integer)2, bobj.field("number"));
            assertEquals((Integer)2, bobj2.field("number"));
        }
    }
}
