/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.eventstorage.memory;

import org.gridgain.testframework.junits.spi.*;

/**
 * Memory event storage SPI config test.
 */
@GridSpiTest(spi = MemoryEventStorageSpi.class, group = "Event Storage SPI")
public class GridMemoryEventStorageSpiConfigSelfTest extends GridSpiAbstractConfigTest<MemoryEventStorageSpi> {
    /**
     * @throws Exception If failed.
     */
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new MemoryEventStorageSpi(), "expireCount", 0);
        checkNegativeSpiProperty(new MemoryEventStorageSpi(), "expireAgeMs", 0);
    }
}
