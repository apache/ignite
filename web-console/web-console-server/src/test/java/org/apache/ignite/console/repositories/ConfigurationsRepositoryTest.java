

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
