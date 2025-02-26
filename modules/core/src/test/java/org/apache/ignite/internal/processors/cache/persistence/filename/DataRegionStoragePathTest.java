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

package org.apache.ignite.internal.processors.cache.persistence.filename;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test cases when {@link DataRegionConfiguration#setStoragePath(String)} used to set custom data region storage path.
 */
public class DataRegionStoragePathTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg.getDefaultDataRegionConfiguration().setStoragePath("dflt_dr");

        dsCfg.setDataRegionConfigurations(
            new DataRegionConfiguration().setName("custom-storage").setStoragePath("custom-storage"),
            new DataRegionConfiguration().setName("default-storage")
        );

        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(dsCfg)
            .setCacheConfiguration(
                ccfg("cache1", null, null),
                ccfg("cache2", "grp1", null),
                ccfg("cache3", "grp1", null),
                ccfg("cache4", null, "default-storage"),
                ccfg("cache5", "grp2", "default-storage"),
                ccfg("cache6", null, "custom-storage"),
                ccfg("cache7", "grp3", "custom-storage"),
                ccfg("cache8", "grp3", "custom-storage")
            );
    }

    /** */
    @Test
    public void testCaches() {

    }

    /** */
    private static CacheConfiguration<?, ?> ccfg(String name, String grp, String dr) {
        return new CacheConfiguration<>(name).setGroupName(grp).setDataRegionName(dr);
    }
}
