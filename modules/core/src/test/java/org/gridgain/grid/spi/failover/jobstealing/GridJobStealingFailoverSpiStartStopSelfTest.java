/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.failover.jobstealing;

import org.gridgain.grid.kernal.*;
import org.gridgain.grid.spi.*;
import org.apache.ignite.spi.collision.jobstealing.*;
import org.gridgain.grid.spi.failover.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.spi.*;
import java.util.*;

/**
 * Job stealing failover SPI start-stop test.
 */
@GridSpiTest(spi = GridJobStealingFailoverSpi.class, group = "Failover SPI")
public class GridJobStealingFailoverSpiStartStopSelfTest extends GridSpiStartStopAbstractTest<GridFailoverSpi> {
    /** {@inheritDoc} */
    @Override protected GridSpiTestContext initSpiContext() throws Exception {
        GridSpiTestContext ctx = super.initSpiContext();

        GridTestNode locNode = new GridTestNode(UUID.randomUUID());

        addSpiDependency(locNode);

        ctx.setLocalNode(locNode);

        return ctx;
    }

    /**
     * Adds Failover SPI attribute.
     *
     * @param node Node to add attribute to.
     */
    private void addSpiDependency(GridTestNode node) {
        node.addAttribute(GridNodeAttributes.ATTR_SPI_CLASS, JobStealingCollisionSpi.class.getName());
    }
}
