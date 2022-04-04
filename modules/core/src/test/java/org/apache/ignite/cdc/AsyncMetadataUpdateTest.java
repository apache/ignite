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
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;

/**
 * Check async update of metadata.
 */
public class AsyncMetadataUpdateTest extends AbstractCdcTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));
    }

    /** */
    @Test
    public void testAsyncBinaryMetaUpdate() throws Exception {
        BinaryObject bobj;
        BinaryMetadata btype;

        try (IgniteEx ignite = startGrid(0)) {
            ignite.cluster().state(ACTIVE);

            BinaryObjectBuilder bldr = ignite.binary().builder("MyType");

            bldr.setField("one", "two");
            bldr.setField("number", 2);

            // Build some object.
            bobj = bldr.build();

            // Get type description.
            btype = ((BinaryTypeImpl)ignite.binary().type("MyType")).metadata();

            assertNotNull(btype);
        }

        // Cleaning metadata.
        cleanPersistenceDir();

        try (IgniteEx ignite = startGrid(0)) {
            ignite.cluster().state(ACTIVE);

            IgniteCache<BinaryObject, BinaryObject> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME).withKeepBinary();

            cache.put(bobj, bobj);

        }
    }
}
