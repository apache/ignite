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

import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Test;

/**
 */
public class BinaryMetadataRemoveWithPersistenceTest extends BinaryMetadataRemoveTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));
    }


    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void startCluster() throws Exception {
        super.startCluster();

        grid("srv0").cluster().active(true);
    }

    /**
     * Remove type metadata and restart cluster.
     */
    @Test
    public void testRemoveTypeAndClusterRestart() throws Exception {
        for (String nodeName : new String[]{"srv0", "srv2", "cli0"}) {
            log.info("+++ Check on " + nodeName);

            BinaryObjectBuilder builder0 = grid(nodeName).binary().builder("Type0");

            builder0.setField("f", 1);
            builder0.build();

            delayIfClient(grid(nodeName));

            removeType(grid(nodeName), "Type0");

            delayIfClient(grid(nodeName));

            stopAllGrids();

            startCluster();

            BinaryObjectBuilder builder1 = grid(nodeName).binary().builder("Type0");
            builder1.setField("f", "string");
            builder1.build();

            delayIfClient(grid(nodeName));

            removeType(grid(nodeName), "Type0");

            delayIfClient(grid(nodeName));
        }
    }
}
