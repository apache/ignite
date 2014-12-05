/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.spi.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

/**
 * Tests manager with {@link GridSpiNoop} SPI's.
 */
public class GridNoopManagerSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    public void testEnabledManager() {
        GridTestKernalContext ctx = new GridTestKernalContext(new GridStringLogger());

        assertTrue(new Manager(ctx, new Spi()).enabled());
        assertFalse(new Manager(ctx, new NoopSpi()).enabled());
        assertTrue(new Manager(ctx, new Spi(), new NoopSpi()).enabled());
        assertTrue(new Manager(ctx, new NoopSpi(), new Spi()).enabled());
    }

    /**
     *
     */
    private static class Manager extends GridManagerAdapter<IgniteSpi> {
        /**
         * @param ctx  Kernal context.
         * @param spis Specific SPI instance.
         */
        protected Manager(GridKernalContext ctx, IgniteSpi... spis) {
            super(ctx, spis);
        }

        /** {@inheritDoc} */
        @Override public void start() throws GridException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void stop(boolean cancel) throws GridException {
            // No-op.
        }
    }

    /**
     *
     */
    private static interface TestSpi extends IgniteSpi {
        // No-op.
    }

    /**
     *
     */
    private static class Spi extends IgniteSpiAdapter implements TestSpi {
        /** {@inheritDoc} */
        @Override public void spiStart(@Nullable String gridName) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // No-op.
        }
    }

    /**
     *
     */
    @GridSpiNoop
    private static class NoopSpi extends Spi {
        // No-op.
    }
}
