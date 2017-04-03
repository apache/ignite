package org.apache.ignite.internal;

import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.eventstorage.NoopEventStorageSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;


/**
 * Event storage tests with default no-op spi.
 *
 */
@GridCommonTest(group = "Kernal Self")
public class GridEventStorageDefaultExceptionTest  extends GridCommonAbstractTest {

    /** */
    public GridEventStorageDefaultExceptionTest() {
        super(/*start grid*/true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setEventStorageSpi(new NoopEventStorageSpi());

        return cfg;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testLocalNodeEventStorage() throws Exception {
        try {
            grid().events().localQuery(F.<Event>alwaysTrue());

            assert false : "Exception should be thrown";
        }
        catch (IgniteException e) {
            assert e.getMessage().startsWith("Local event query called with default noop event storage spi") : "Wrong exception message." + e.getMessage();
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemoteNodeEventStorage() throws Exception {
        try {
            grid().events().remoteQuery(F.<Event>alwaysTrue(), 0);

            assert false : "Exception should be thrown";
        }
        catch (IgniteException e) {
            assert e.getMessage().startsWith("Remote event query called with default noop event storage spi") : "Wrong exception message." + e.getMessage();
        }
    }
}
