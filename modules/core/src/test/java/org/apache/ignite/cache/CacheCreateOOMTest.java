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
 *
 */

package org.apache.ignite.cache;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryAllocator;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.junit.Assume.assumeTrue;

/** */
@RunWith(Parameterized.class)
public class CacheCreateOOMTest extends GridCommonAbstractTest {
    private static final OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();

    /** */
    private static final String DEFAULT_DATA_REGION_NAME = "default";

    /** */
    private static final String DEFAULT_CACHE_NAME = "default_cache";

    /** */
    private static final String CUSTOM_DATA_REGION_NAME = "custom";

    /** */
    private static final String CUSTOM_CACHE_NAME = "custom_cache";

    /** */
    @Parameterized.Parameters(name = "withPersistance={0}, withCustomDataRegion={1}")
    public static List<Object[]> parameters() {
        return ImmutableList.of(
            new Object[]{true, true},
            new Object[]{true, false},
            new Object[]{false, false},
            new Object[]{false, true}
        );
    }

    /** */
    @Parameterized.Parameter()
    public boolean withPersistance;

    /** */
    @Parameterized.Parameter(1)
    public boolean withCustomDataRegion;

    /** */
    public long nodeMaxRAM;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
//            .setMemoryAllocator(new CustomMemoryAllocator());

        if (withCustomDataRegion)
            storageCfg.setDataRegionConfigurations(getMaxedDataRegionConfiguration(CUSTOM_DATA_REGION_NAME));
        else
            storageCfg.setDefaultDataRegionConfiguration(getMaxedDataRegionConfiguration(DEFAULT_DATA_REGION_NAME));

        cfg.setDataStorageConfiguration(storageCfg);

        cfg.setCacheConfiguration(
            getCacheConfiguration(DEFAULT_CACHE_NAME, DEFAULT_DATA_REGION_NAME),
            getCacheConfiguration(CUSTOM_CACHE_NAME, CUSTOM_DATA_REGION_NAME)
        );

        return cfg;
    }

    /** */
    private CacheConfiguration<?, ?> getCacheConfiguration(String cacheName, String dataRegionName) {
        return new CacheConfiguration<>(cacheName).setDataRegionName(dataRegionName);
    }

    private DataRegionConfiguration getMaxedDataRegionConfiguration(String dataRegionName) {
        return new DataRegionConfiguration()
            .setName(dataRegionName)
            .setInitialSize(2 * nodeMaxRAM)
            .setMaxSize(2 * nodeMaxRAM)
            .setPersistenceEnabled(withPersistance);
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        assumeTrue(os instanceof com.sun.management.OperatingSystemMXBean);

        nodeMaxRAM = ((com.sun.management.OperatingSystemMXBean)os).getTotalPhysicalMemorySize();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testCheckExceptionOnCacheFailure() {
        assertThrowsAnyCause(log, this::startGrid, IgniteOutOfMemoryException.class,
            "Adjust the heap settings or data storage configuration to allocate the memory");
    }

    /** */
    private static class CustomMemoryAllocator extends UnsafeMemoryAllocator implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public long allocateMemory(long size) {
            throw new OutOfMemoryError();
        }
    }
}
