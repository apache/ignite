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

package org.apache.ignite.console.repositories;

import org.apache.ignite.console.web.model.ConfigurationKey;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.UUID;

/**
 * Configurations repository test.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class ConfigurationsRepositoryTest {
    /** Configurations repository. */
    @Autowired
    private ConfigurationsRepository configurationsRepo;

    /**
     * Should throw cluster not found exception.
     */
    @Test
    public void testClusterNotFoundExceptionDuringLoadingConfiguration() {
        UUID accId = UUID.randomUUID();
        UUID clusterId = UUID.randomUUID();

        GridTestUtils.assertThrows(null, () -> {
            configurationsRepo.loadConfiguration(new ConfigurationKey(accId, false), clusterId);
            return null;
        }, IllegalStateException.class, "Cluster not found for ID: " + clusterId);
    }

    /**
     * Should throw cluster not found exception.
     */
    @Test
    public void testClusterNotFoundExceptionDuringLoadCluster() {
        UUID accId = UUID.randomUUID();
        UUID clusterId = UUID.randomUUID();

        GridTestUtils.assertThrows(null, () -> {
            configurationsRepo.loadCluster(new ConfigurationKey(accId, false), clusterId);
            return null;
        }, IllegalStateException.class, "Cluster not found for ID: " + clusterId);
    }

    /**
     * Should throw cache not found exception.
     */
    @Test
    public void testCacheNotFoundExceptionDuringLoadCache() {
        UUID accId = UUID.randomUUID();
        UUID cacheId = UUID.randomUUID();

        GridTestUtils.assertThrows(null, () -> {
            configurationsRepo.loadCache(new ConfigurationKey(accId, false), cacheId);
            return null;
        }, IllegalStateException.class, "Cache not found for ID: " + cacheId);
    }

    /**
     * Should throw cache not found exception.
     */
    @Test
    public void testModelNotFoundExceptionDuringLoadCache() {
        UUID accId = UUID.randomUUID();
        UUID mdlId = UUID.randomUUID();

        GridTestUtils.assertThrows(null, () -> {
            configurationsRepo.loadModel(new ConfigurationKey(accId, false), mdlId);
            return null;
        }, IllegalStateException.class, "Model not found for ID: " + mdlId);
    }
}
