/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.communication.tcp;

import org.gridgain.grid.spi.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.spi.*;

/**
 * TCP communication SPI config start-stop test.
 */
@GridSpiTest(spi = TcpCommunicationSpi.class, group = "Communication SPI")
public class GridTcpCommunicationSpiStartStopSelfTest extends GridSpiStartStopAbstractTest<TcpCommunicationSpi> {
    /**
     * @return Local port.
     * @throws Exception If failed.
     */
    @GridSpiTestConfig
    public int getLocalPort() throws Exception {
        return GridTestUtils.getNextCommPort(getClass());
    }
}
