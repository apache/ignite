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

package org.apache.ignite.internal.processors.configuration.distributed;

import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.junit.Assume.assumeTrue;

/**
 *
 */
public abstract class DistributedConfigurationAbstractTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_PROP = "someLong";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        TestDistibutedConfigurationPlugin.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();

        TestDistibutedConfigurationPlugin.clear();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(isPersistent())
            .setMaxSize(500L * 1024 * 1024);

        cfg.setDataStorageConfiguration(storageCfg);

        return cfg;
    }

    /** */
    protected abstract boolean isPersistent();

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSuccessClusterWideUpdate() throws Exception {
        assumeTrue(isPersistent());

        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        ignite0.cluster().active(true);

        DistributedLongProperty long0 = ignite0.context().distributedConfiguration().registerLong(TEST_PROP);
        DistributedLongProperty long1 = ignite1.context().distributedConfiguration().registerLong(TEST_PROP);

        long0.propagate(0L);

        assertEquals(0, long0.get().longValue());
        assertEquals(0, long1.get().longValue());

        assertTrue(long0.propagate(2L));

        //Value changed on whole grid.
        assertEquals(2L, long0.get().longValue());
        assertEquals(2L, long1.get().longValue());

        stopAllGrids();

        ignite0 = startGrid(0);
        ignite1 = startGrid(1);

        ignite0.cluster().active(true);

        long0 = ignite0.context().distributedConfiguration().registerLong(TEST_PROP);
        long1 = ignite1.context().distributedConfiguration().registerLong(TEST_PROP);

        assertEquals(2, long0.get().longValue());
        assertEquals(2, long1.get().longValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReadLocalValueOnInactiveGrid() throws Exception {
        assumeTrue(isPersistent());

        IgniteEx ignite0 = startGrid(0);
        startGrid(1);

        ignite0.cluster().active(true);

        DistributedLongProperty long0 = ignite0.context().distributedConfiguration().registerLong(TEST_PROP);

        long0.propagate(0L);

        assertEquals(0, long0.get().longValue());

        assertTrue(long0.propagate(2L));

        stopAllGrids();

        ignite0 = startGrid(0);

        long0 = ignite0.context().distributedConfiguration().registerLong(TEST_PROP);

        assertEquals(2, long0.get().longValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRegisterExistedProperty() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        ignite0.cluster().active(true);

        DistributedLongProperty long0 = ignite0.context().distributedConfiguration().registerLong(TEST_PROP);

        long0.propagate(0L);

        assertEquals(0, long0.get().longValue());

        assertTrue(long0.propagate(2L));

        DistributedLongProperty long1 = ignite1.context().distributedConfiguration().registerLong(TEST_PROP);

        //Already changed to 2.
        assertEquals(2, long1.get().longValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test(expected = DetachedPropertyException.class)
    public void testNotAttachedProperty() throws Exception {
        DistributedLongProperty long0 = DistributedLongProperty.detachedLongProperty(TEST_PROP);

        long0.propagate(1L);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPropagateValueOnInactiveGridShouldNotThrowException() throws Exception {
        assumeTrue(isPersistent());

        IgniteEx ignite0 = (IgniteEx)startGrids(2);

        DistributedLongProperty long0 = ignite0.context().distributedConfiguration().registerLong(TEST_PROP);

        long0.propagate(2L);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRegisterPropertyBeforeOnReadyForReadHappened() throws Exception {
        assumeTrue(isPersistent());

        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        ignite0.cluster().active(true);

        DistributedLongProperty long0 = ignite0.context().distributedConfiguration().registerLong(TEST_PROP);

        long0.propagate(0L);

        assertEquals(0, long0.get().longValue());

        long0.propagate(2L);

        stopAllGrids();

        TestDistibutedConfigurationPlugin.supplier = (ctx) -> {
            ctx.distributedConfiguration().registerLong(TEST_PROP);
        };

        ignite0 = startGrid(0);
        ignite1 = startGrid(1);

        long0 = ignite0.context().distributedConfiguration().getProperty(TEST_PROP);
        DistributedLongProperty long1 = ignite1.context().distributedConfiguration().getProperty(TEST_PROP);

        //After start it should read from local storage.
        assertEquals(2, long0.get().longValue());
        assertEquals(2, long1.get().longValue());
    }

}
