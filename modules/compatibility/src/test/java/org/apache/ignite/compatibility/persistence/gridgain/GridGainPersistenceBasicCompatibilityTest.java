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

package org.apache.ignite.compatibility.persistence.gridgain;

import java.lang.reflect.Method;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.ignite.compatibility.persistence.PersistenceBasicCompatibilityTest.ConfigurationClosure;
import static org.apache.ignite.compatibility.persistence.PersistenceBasicCompatibilityTest.PostStartupClosure;
import static org.apache.ignite.compatibility.persistence.PersistenceBasicCompatibilityTest.TEST_CACHE_NAME;
import static org.apache.ignite.compatibility.persistence.PersistenceBasicCompatibilityTest.validateResultingCacheData;

/**
 * Saves data using GridGain and then load this data using actual Ignite version.
 */
public class GridGainPersistenceBasicCompatibilityTest extends GridgainPersistenceCompatibilityAbstractTest {
    /**
     * Test that Ignite can read persistence cache data from previously deployed GridGain.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReadPersistenceDataTwoNodes() throws Exception {
        startGrid(1, ggVer, new ConfigurationClosure(true), new CI1<Ignite>() {
            @Override public void apply(Ignite ignite) {
                // No-op.
            }
        });

        startGrid(2, ggVer, new ConfigurationClosure(true), new PostStartupClosure());

        stopAllGrids();

        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        ignite0.cluster().state(ClusterState.ACTIVE);

        assertEquals(2, ignite0.context().discovery().aliveServerNodes().size());

        validateResultingCacheData(ignite0.cache(TEST_CACHE_NAME));
        validateResultingCacheData(ignite1.cache(TEST_CACHE_NAME));
    }

    /**
     * Test that Ignite can read metastorage data from previously deployed GridGain.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReadMetastorage() throws Exception {
        Assume.assumeTrue("Distributed metastorage is available since version 8.7.12",
            compareVersions(ggVer, "8.7.12") >= 0);

        String testKey = "testKey";
        String testValue = "testValue";

        // Start GridGain and fill metastorage.
        startGrid(1, ggVer, new ConfigurationClosure(true), new CIX1<Ignite>() {
            @Override public void applyx(Ignite grid) throws IgniteCheckedException {
                IgniteEx ignite = (IgniteEx)grid;

                ignite.active(true);

                ignite.context().distributedMetastorage().write(testKey, testValue);

                assertEquals(testValue, ignite.context().distributedMetastorage().read(testKey));
            }
        });

        stopAllGrids();

        // Start Ignite and validate metastorage.
        IgniteEx ignite = startGrid(0);

        assertEquals(1, ignite.context().discovery().topologyVersion());

        ignite.cluster().state(ClusterState.ACTIVE);

        DistributedMetaStorage distMetaStorage = ignite.context().distributedMetastorage();

        distMetaStorage.iterate("",
            (key, value) -> log.info("Read metastorage entry [key=" + key + ", value=" + value + ']'));

        assertEquals(testValue, distMetaStorage.read(testKey));
    }

    /**
     * Test starting Ignite after setting up cluster id and tag.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testChangeClusterIdAndTag() throws Exception {
        Assume.assumeTrue("Cluster id and tag is available since version 8.7.12",
            compareVersions(ggVer, "8.7.12") >= 0);

        // Start GridGain and set up the cluster tag.
        startGrid(1, ggVer, new ConfigurationClosure(true), new CIX1<Ignite>() {
            @Override public void applyx(Ignite grid) throws IgniteCheckedException {
                IgniteEx ignite = (IgniteEx)grid;

                ignite.active(true);

                IgniteCluster cluster = ignite.cluster();

                // Make sure that Cluster ID and Tag feature is supported.
                Method getIdMethod = U.findNonPublicMethod(IgniteClusterEx.class, "id");
                Method setTagMethod = U.findNonPublicMethod(IgniteClusterEx.class, "tag", String.class);
                Method getTagMethod = U.findNonPublicMethod(IgniteClusterEx.class, "tag");

                assertNotNull("Cluster ID and Tag feature is not supported by this version.", getIdMethod);
                assertNotNull("Cluster ID and Tag feature is not supported by this version.", setTagMethod);
                assertNotNull("Cluster ID and Tag feature is not supported by this version.", getTagMethod);

                // Set up the cluster tag.
                String clusterTag = "testTag";

                U.invoke(IgniteClusterEx.class, cluster, "tag", new Class[] {String.class}, clusterTag);

                String tag = U.invoke(IgniteClusterEx.class, cluster, "tag", new Class[] {}, null);

                assertEquals(clusterTag, tag);
            }
        });

        stopAllGrids();

        IgniteEx ignite = startGrid(0);

        startGrid(1);

        ignite.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        assertEquals(2, ignite.context().discovery().allNodes().size());
    }
}
