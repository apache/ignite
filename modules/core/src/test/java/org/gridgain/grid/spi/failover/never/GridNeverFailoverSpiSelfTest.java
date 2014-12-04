/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.failover.never;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.grid.spi.failover.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.spi.*;
import java.util.*;

/**
 * Never failover SPI test.
 */
@GridSpiTest(spi = GridNeverFailoverSpi.class, group = "Failover SPI")
public class GridNeverFailoverSpiSelfTest extends GridSpiAbstractTest<GridNeverFailoverSpi> {
    /**
     * @throws Exception If failed.
     */
    public void testAlwaysNull() throws Exception {
        List<ClusterNode> nodes = new ArrayList<>();

        ClusterNode node = new GridTestNode(UUID.randomUUID());

        nodes.add(node);

        assert getSpi().failover(new GridFailoverTestContext(new GridTestTaskSession(), new GridTestJobResult(node)),
            nodes) == null;
    }
}
