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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.google.common.primitives.Booleans.asList;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.cartesianProduct;

/**
 * Test cache directory name validation.
 */
@RunWith(Parameterized.class)
public class CacheDirectoryNameTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameter
    public boolean persistenceEnabled;

    /** */
    @Parameterized.Parameter(1)
    public boolean checkGroup;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "persistenceEnabled={0}, isGroupName={1}")
    public static Collection<?> parameters() {
        return cartesianProduct(
            asList(false, true), asList(false, true)
        );
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(persistenceEnabled)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testCacheDirectoryContainsInvalidFileNameChars() throws Exception {
        IgniteEx srv = startGrid();

        srv.cluster().state(ClusterState.ACTIVE);

        List<String> illegalNames = new ArrayList<>();

        illegalNames.add("/");
        illegalNames.add("a/b");

        if (U.isWindows()) {
            illegalNames.add("a>b");
            illegalNames.add("a\\b");
        }

        for (String name : illegalNames) {
            CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

            if (checkGroup)
                cfg.setGroupName(name);
            else
                cfg.setName(name);

            if (persistenceEnabled) {
                assertThrows(log, () -> srv.createCache(cfg), IgniteCheckedException.class,
                    "Cache start failed. Cache or group name contains the characters that are not allowed in file names");
            }
            else {
                srv.createCache(cfg);
                srv.destroyCache(cfg.getName());
            }
        }
    }
}
