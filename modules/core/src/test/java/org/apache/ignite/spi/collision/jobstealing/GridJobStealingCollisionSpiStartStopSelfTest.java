/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.collision.jobstealing;

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.GridSpiStartStopAbstractTest;
import org.apache.ignite.spi.failover.jobstealing.JobStealingFailoverSpi;
import org.apache.ignite.testframework.GridSpiTestContext;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SPI_CLASS;

/**
 * Job stealing collision SPI start-stop test.
 */
@GridSpiTest(spi = JobStealingCollisionSpi.class, group = "Collision SPI")
public class GridJobStealingCollisionSpiStartStopSelfTest
    extends GridSpiStartStopAbstractTest<JobStealingCollisionSpi> {
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
     * @throws Exception If failed.
     */
    private void addSpiDependency(GridTestNode node) throws Exception {
        node.addAttribute(U.spiAttribute(getSpi(), ATTR_SPI_CLASS), JobStealingFailoverSpi.class.getName());
    }
}