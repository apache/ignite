package org.apache.ignite.internal.managers.events;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVTS_ALL;

/**
 * Test that local event listener implemets LifecycleAware.
 */
public class LifecicleAwareListenerTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartStop() throws Exception {
        TestLocalListener lsnr = new TestLocalListener();

        IgniteConfiguration cfg = getConfiguration().setLocalEventListeners(F.asMap(lsnr, EVTS_ALL));

        try (Ignite ignite = startGrid(cfg)) {
            assertTrue(lsnr.isStarted);
            assertFalse(lsnr.isStoped);
        }

        assertTrue(lsnr.isStoped);
    }

    /** */
    private static class TestLocalListener implements IgnitePredicate<Event>, LifecycleAware {
        /** Is started. */
        private boolean isStarted;

        /** Is stoped. */
        private boolean isStoped;

        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void start() throws IgniteException {
            assertFalse(isStarted);

            isStarted = true;
        }

        /** {@inheritDoc} */
        @Override public void stop() throws IgniteException {
            assertFalse(isStoped);

            isStoped = true;
        }
    }
}
