/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi;

import org.gridgain.testframework.junits.spi.*;

/**
 * Base SPI start-stop test class.
 * @param <T> SPI implementation class.
 */
public abstract class GridSpiStartStopAbstractTest<T extends IgniteSpi> extends GridSpiAbstractTest<T> {
    /** */
    public static final int COUNT = 5;

    /** Disables autostart. */
    protected GridSpiStartStopAbstractTest() {
        super(false);
    }

    /**
     * @return Count.
     */
    protected int getCount() {
        return COUNT;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testStartStop() throws Exception {
        info("Spi start-stop test [count=" + getCount() + ", spi=" + getSpiClass().getSimpleName() + ']');

        spiStart();

        try {
            for (int i = 0; i < getCount(); i++) {
                getSpi().onContextDestroyed();
                getSpi().spiStop();

                // Call to get node attributes as part of life cycle.
                getSpi().getNodeAttributes();

                getSpi().spiStart(getTestGridName());
                getSpi().onContextInitialized(getSpiContext());
            }
        }
        finally {
            spiStop();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testStop() throws Exception {
        IgniteSpi spi = getSpiClass().newInstance();

        getTestResources().inject(spi);

        spi.spiStop();
    }
}
