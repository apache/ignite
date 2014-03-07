/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing.h2;

import org.gridgain.grid.spi.*;
import org.gridgain.testframework.junits.spi.*;

/**
 * Start/stop tests for H2 indexing SPI.
 */
@GridSpiTest(spi = GridH2IndexingSpi.class, group = "Indexing SPI")
public class GridH2IndexingSpiInMemStartStopSelfTest extends GridSpiStartStopAbstractTest<GridH2IndexingSpi>{
    // No-op.
}
