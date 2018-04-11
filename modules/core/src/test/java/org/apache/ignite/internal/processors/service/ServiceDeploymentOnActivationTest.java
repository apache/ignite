package org.apache.ignite.internal.processors.service;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Check, that services are redeployed, when cluster is deactivated and activated back again.
 */
public class ServiceDeploymentOnActivationTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true).setMaxSize(10 * 1024 * 1024)
            ).setWalMode(WALMode.LOG_ONLY)
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /**
     *
     * @throws Exception If failed.
     */
    public void testServiceDeploymentOnActivation() throws Exception {
        Ignite ignite = startGrid(1);

        ignite.cluster().active(true);

        String svcName = "test-service";

        CountDownLatch exeLatch = new CountDownLatch(1);
        CountDownLatch cancelLatch = new CountDownLatch(1);

        DummyService.exeLatch(svcName, exeLatch);
        DummyService.cancelLatch(svcName, cancelLatch);

        ignite.services().deployNodeSingleton(svcName, new DummyService());

        assertTrue(exeLatch.await(10, TimeUnit.SECONDS));

        ignite.cluster().active(false);

        assertTrue(cancelLatch.await(10, TimeUnit.SECONDS));

        exeLatch = new CountDownLatch(1);

        DummyService.exeLatch(svcName, exeLatch);

        ignite.cluster().active(true);

        assertTrue(exeLatch.await(10, TimeUnit.SECONDS));
    }
}
