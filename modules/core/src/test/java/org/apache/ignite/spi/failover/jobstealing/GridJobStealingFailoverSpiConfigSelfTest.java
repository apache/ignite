/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.failover.jobstealing;

import org.gridgain.testframework.junits.spi.*;

/**
 * Job stealing failover SPI config test.
 */
@GridSpiTest(spi = JobStealingFailoverSpi.class, group = "Collision SPI")
public class GridJobStealingFailoverSpiConfigSelfTest extends GridSpiAbstractConfigTest<JobStealingFailoverSpi> {
    /**
     * @throws Exception If failed.
     */
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new JobStealingFailoverSpi(), "maximumFailoverAttempts", -1);
    }
}
