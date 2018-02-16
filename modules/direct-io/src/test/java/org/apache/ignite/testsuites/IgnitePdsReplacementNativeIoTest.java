package org.apache.ignite.testsuites;

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgnitePdsPageReplacementTest;

/**
 * Page replacement light variant of test for native direct IO (wastes real IOPs on agents)
 */
public class IgnitePdsReplacementNativeIoTest extends IgnitePdsPageReplacementTest {

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 15 * 60 * 1000;
    }

    /** {@inheritDoc} */
    @Override protected int getPagesNum() {
        // 1k - passed, 20k - passed, 64k - failed
        return 20 * 1024;
    }

    /** {@inheritDoc} */
    @Override public void testPageReplacement() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_USE_ASYNC_FILE_IO_FACTORY, "false");

        super.testPageReplacement();
    }
}
