package org.apache.ignite.internal.processors.cache.persistence;

import java.util.concurrent.Callable;
import javax.cache.configuration.Factory;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgnitePdsCorruptedCacheDataTest extends GridCommonAbstractTest {
    /** Error message */
    private static final String ERR_MESSAGE = "During cache configuration loading from given file occurred error. " +
        "Make sure that user library containing required class is valid. " +
        "If library is valid then delete cache configuration file and restart cache";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration res = super.getConfiguration(igniteInstanceName);

        res.setConsistentId(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        dsCfg.setDefaultDataRegionConfiguration(
            new DataRegionConfiguration()
                .setMaxSize(1024 * 1024 * 1024)
                .setPersistenceEnabled(true)
        );

        res.setDataStorageConfiguration(dsCfg);

        return res;
    }

    /** */
    @SuppressWarnings("unchecked")
    private CacheConfiguration getCacheConfiguration(ClassLoader clsLdr) throws Exception {
        CacheConfiguration res = new CacheConfiguration();
        res.setName("test_cache");

        Factory storeFactory = (Factory)clsLdr.loadClass("org.apache.ignite.tests.p2p.CacheDeploymentTestStoreFactory")
            .newInstance();
        res.setCacheStoreFactory(storeFactory);

        return res;
    }

    /**
     * @throws Exception if failed.
     */
    public void testFilePageStoreManagerShouldThrowExceptionWhenFactoryClassCannotBeLoaded() throws Exception {
        String instanceName = getTestIgniteInstanceName(0);

        ClassLoader externalClsLdr = getExternalClassLoader();

        IgniteConfiguration cfg = optimize(getConfiguration(instanceName));
        cfg.setCacheConfiguration(getCacheConfiguration(externalClsLdr));
        cfg.setClassLoader(externalClsLdr);

        IgniteEx ignite = (IgniteEx)startGrid(instanceName, cfg, null);

        ignite.cluster().active(true);

        ignite.context().config().setClassLoader(U.gridClassLoader());
        GridCacheSharedContext sharedCtx = ignite.context().cache().context();
        FilePageStoreManager pageStore = (FilePageStoreManager)sharedCtx.pageStore();

        assertNotNull(pageStore);

        Throwable e = GridTestUtils.assertThrowsWithCause((Callable<Void>)() -> {
                pageStore.readCacheConfigurations();
                return null;
            },
            ClassNotFoundException.class);

        assertNotNull(e);

        assertTrue(e.getMessage().startsWith(ERR_MESSAGE));
    }
}
