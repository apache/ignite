package org.apache.ignite.testsuites;

import org.apache.ignite.internal.processors.cache.persistence.db.file.IgnitePdsEvictionTest;

//todo remove
public class IgnitePdsEvictionTest2 extends IgnitePdsEvictionTest {

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 15 * 60 * 1000;
    }

    /** {@inheritDoc} */
    protected int getPagesNum() {
        return 1024;
    }

    @Override public void testPageEviction() throws Exception {
        super.testPageEviction();
    }
}
