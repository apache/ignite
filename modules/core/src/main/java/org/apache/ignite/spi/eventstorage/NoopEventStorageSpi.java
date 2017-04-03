package org.apache.ignite.spi.eventstorage;

import org.apache.ignite.events.Event;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;

/**
 * No-op implementation of {@link org.apache.ignite.spi.eventstorage.EventStorageSpi}.
 */
@IgniteSpiMultipleInstancesSupport(true)
public class NoopEventStorageSpi extends IgniteSpiAdapter implements EventStorageSpi {

    /** {@inheritDoc} */
    @Override public <T extends Event> Collection<T> localEvents(IgnitePredicate<T> p) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void record(Event evt) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String gridName) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        // No-op.
    }
}
