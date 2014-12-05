/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.eventstorage.memory;

import org.gridgain.grid.spi.*;
import org.gridgain.testframework.junits.spi.*;

/**
 * Memory event storage SPI start-stop test.
 */
@GridSpiTest(spi = MemoryEventStorageSpi.class, group = "Event Storage SPI")
public class GridMemoryEventStorageSpiStartStopSelfTest extends GridSpiStartStopAbstractTest<MemoryEventStorageSpi> {
    // No-op.
}
