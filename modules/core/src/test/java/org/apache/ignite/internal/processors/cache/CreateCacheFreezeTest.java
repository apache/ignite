package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class CreateCacheFreezeTest extends GridCommonAbstractTest {
    public void test() throws Exception {
        IgniteEx ignite = startGrid(0);

        U.registerMBean(ignite.context().config().getMBeanServer(),
            ignite.name(), "FIRST_CACHE",

            "org.apache.ignite.internal.processors.cache.CacheLocalMetricsMXBeanImpl",
            new DummyMBeanImpl(), DummyMBean.class);

        GridTestUtils.assertThrowsWithCause(() -> {
            ignite.createCache(new CacheConfiguration<>("FIRST_CACHE"));

            return 0;
        }, IgniteCheckedException.class);
        //The creation of SECOND_CACHE will hang because of ExchangeWorker
        assertNotNull(ignite.createCache(new
            CacheConfiguration<>("SECOND_CACHE")));
    }

    public interface DummyMBean {
        void noop();
    }
    static class DummyMBeanImpl implements DummyMBean {
        @Override public void noop() {
        }
    }
}