/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.checkpoint.sharedfs;

import org.gridgain.grid.spi.*;
import org.gridgain.testframework.junits.spi.*;

/**
 * Grid shared file system checkpoint SPI start stop self test.
 */
@GridSpiTest(spi = SharedFsCheckpointSpi.class, group = "Collision SPI")
public class GridSharedFsCheckpointSpiStartStopSelfTest extends GridSpiStartStopAbstractTest<SharedFsCheckpointSpi> {
    // No-op.
}
