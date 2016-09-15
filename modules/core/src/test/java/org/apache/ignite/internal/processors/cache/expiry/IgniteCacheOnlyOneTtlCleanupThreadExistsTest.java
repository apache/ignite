package org.apache.ignite.internal.processors.cache.expiry;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.regex.Pattern;

/**
 * One and only one Ttl cleanup worker thread must exists only
 * if al least one cache with set 'eagerTtl' flag exists.
 */
public class IgniteCacheOnlyOneTtlCleanupThreadExistsTest extends GridCommonAbstractTest {
    private static final String CACHE_NAME1 = "cache-1";
    private static final String CACHE_NAME2 = "cache-2";

    public void testOnlyOneTtlCleanupThreadExists() throws Exception {
        try (final Ignite g = startGrid(0)) {
            checkCleanupThreadExists(false);

            g.createCache(createCacheConfiguration(CACHE_NAME1, false));

            checkCleanupThreadExists(false);

            g.createCache(createCacheConfiguration(CACHE_NAME2, true));

            checkCleanupThreadExists(true);

            g.destroyCache(CACHE_NAME1);

            checkCleanupThreadExists(true);

            g.createCache(createCacheConfiguration(CACHE_NAME1, true));

            checkCleanupThreadExists(true);

            g.destroyCache(CACHE_NAME1);

            checkCleanupThreadExists(true);

            g.destroyCache(CACHE_NAME2);

            checkCleanupThreadExists(false);
        }
    }

    private CacheConfiguration createCacheConfiguration(String name, boolean eagerTtl) {
        CacheConfiguration ccfg = new CacheConfiguration();
        ccfg.setEagerTtl(eagerTtl);
        ccfg.setName(name);

        return ccfg;
    }

    private void checkCleanupThreadExists(boolean exists) throws Exception {
        int count = 0;
        Pattern pattern = Pattern.compile("ttl-cleanup-worker-#\\d+%" + getTestGridName(0) + "%");
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (pattern.matcher(t.getName()).matches())
                count++;
        }
        if (count > 1)
            fail("More then one ttl cleanup worker threads exists");
        if (exists) {
            assertEquals("Ttl cleanup thread don't exists", count, 1);
        } else {
            assertEquals("Ttl cleanup thread exists", count, 0);
        }
    }
}
