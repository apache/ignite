package org.apache.ignite.internal.processors.security.client;

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * Runs operations of a thin client to check that thin client security context is
 * available on local and remote nodes.
 */
public class ThinClientSecurityContextOnRemoteNodeTest extends ThinClientPermissionCheckTest {
    /** {@inheritDoc} */
    @Override protected CacheConfiguration[] cacheConfigurations() {
        return new CacheConfiguration[] {
            new CacheConfiguration().setName(CACHE).setCacheMode(CacheMode.REPLICATED),
            new CacheConfiguration().setName(FORBIDDEN_CACHE)
        };
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridAllowAll("srv");

        super.beforeTestsStarted();
    }
}
