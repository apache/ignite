package org.apache.ignite.internal.metric;

import java.util.Arrays;
import java.util.Collection;
import javax.cache.expiry.Duration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.platform.cache.expiry.PlatformExpiryPolicyFactory;
import org.apache.ignite.spi.systemview.view.CacheView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHES_VIEW;

/** Tests for {@link CacheView} expiry policy factory representation */
@RunWith(Parameterized.class)
public class SystemViewCacheExpiryPolicyTest extends GridCommonAbstractTest {
    /**Create ttl {@link Duration}*/
    @Parameterized.Parameter
    public long create;

    /** Access ttl {@link Duration}*/
    @Parameterized.Parameter(1)
    public long update;

    /** Update ttl {@link Duration}*/
    @Parameterized.Parameter(2)
    public long access;

    /** Anticipated {@link String} expiry policy factory representation*/
    @Parameterized.Parameter(3)
    public String actual;

    /**
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "create={0}, update={1}, access={2}, actual={3}")
    public static Collection parameters() {
        return Arrays.asList(new Object[][] {
            {2, 4, 8, "[create=2MILLISECONDS][update=4MILLISECONDS][access=8MILLISECONDS]"},
            {1, -2, -1, "[create=1MILLISECONDS]"},
            {-1, 0, -1, "Eternal"},
            {0, 1, -1, "[update=1MILLISECONDS]"}
        });
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        cleanPersistenceDir();
    }

    /**
     * Test for {@link CacheView} expiry policy factory representation. The test initializes the {@link CacheConfiguration}
     * with custom {@link PlatformExpiryPolicyFactory}. Given different ttl input, the test checks the {@link String}
     * expiry policy factory outcome for {@link CacheView#expiryPolicyFactory()}.
     */
    @Test
    public void testCacheViewExpiryPolicy() throws Exception {
        try (IgniteEx g = startGrid()) {
            CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>();
            ccfg.setName("cache");
            ccfg.setExpiryPolicyFactory(new PlatformExpiryPolicyFactory(create, update, access));

            g.getOrCreateCache(ccfg);

            SystemView<CacheView> caches = g.context().systemView().view(CACHES_VIEW);

            for (CacheView row : caches)
                if (row.cacheName().equals("cache"))
                    assertEquals(actual, row.expiryPolicyFactory());
        }
    }
}
