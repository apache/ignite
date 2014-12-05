/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.failover.always;

import org.gridgain.testframework.junits.spi.*;

/**
 * Always-failover SPI config test.
 */
@GridSpiTest(spi = AlwaysFailoverSpi.class, group = "Collision SPI")
public class GridAlwaysFailoverSpiConfigSelfTest extends GridSpiAbstractConfigTest<AlwaysFailoverSpi> {
    /**
     * @throws Exception If failed.
     */
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new AlwaysFailoverSpi(), "maximumFailoverAttempts", -1);
    }
}
