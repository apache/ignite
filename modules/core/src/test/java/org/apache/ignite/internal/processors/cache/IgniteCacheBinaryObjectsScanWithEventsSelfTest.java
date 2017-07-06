package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.events.EventType;

/**
 *
 */
public class IgniteCacheBinaryObjectsScanWithEventsSelfTest extends IgniteCacheBinaryObjectsScanSelfTest {
    /** {@inheritDoc} */
    @Override protected int[] getIncludeEventTypes() {
        return EventType.EVTS_ALL;
    }
}
