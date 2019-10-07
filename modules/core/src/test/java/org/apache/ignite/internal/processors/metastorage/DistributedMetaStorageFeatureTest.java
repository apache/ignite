/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.metastorage;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISTRIBUTED_META_STORAGE_FEATURE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** */
public class DistributedMetaStorageFeatureTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        return cfg;
    }

    /** */
    @Before
    public void before() throws Exception {
        stopAllGrids();
    }

    /** */
    @After
    public void after() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void testFeatureDisabled() throws Exception {
        testFeatureDisabled0();
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_DISTRIBUTED_META_STORAGE_FEATURE, value = "false")
    public void testFeatureExplicitlyDisabled() throws Exception {
        testFeatureDisabled0();
    }

    /** */
    protected void testFeatureDisabled0() throws Exception {
        IgniteEx ignite = startGrid(0);

        //noinspection ThrowableNotThrown
        assertThrows(log, ignite.context()::distributedMetastorage, UnsupportedOperationException.class, "");

        assertFalse(IgniteFeatures.nodeSupports(
            ignite.localNode().attribute(IgniteNodeAttributes.ATTR_IGNITE_FEATURES),
            IgniteFeatures.DISTRIBUTED_METASTORAGE
        ));
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_DISTRIBUTED_META_STORAGE_FEATURE, value = "true")
    public void testFeatureEnabled() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.context().distributedMetastorage();

        assertTrue(IgniteFeatures.nodeSupports(
            ignite.localNode().attribute(IgniteNodeAttributes.ATTR_IGNITE_FEATURES),
            IgniteFeatures.DISTRIBUTED_METASTORAGE
        ));
    }
}
