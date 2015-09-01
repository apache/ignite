/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.failover.jobstealing;

import java.util.UUID;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.spi.GridSpiStartStopAbstractTest;
import org.apache.ignite.spi.collision.jobstealing.JobStealingCollisionSpi;
import org.apache.ignite.spi.failover.FailoverSpi;
import org.apache.ignite.testframework.GridSpiTestContext;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;

/**
 * Job stealing failover SPI start-stop test.
 */
@GridSpiTest(spi = JobStealingFailoverSpi.class, group = "Failover SPI")
public class GridJobStealingFailoverSpiStartStopSelfTest extends GridSpiStartStopAbstractTest<FailoverSpi> {
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
        node.addAttribute(IgniteNodeAttributes.ATTR_SPI_CLASS, JobStealingCollisionSpi.class.getName());
    }
}