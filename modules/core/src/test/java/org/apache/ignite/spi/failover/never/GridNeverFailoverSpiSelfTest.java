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

package org.apache.ignite.spi.failover.never;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.GridTestJobResult;
import org.apache.ignite.GridTestTaskSession;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.spi.failover.GridFailoverTestContext;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;

/**
 * Never failover SPI test.
 */
@GridSpiTest(spi = NeverFailoverSpi.class, group = "Failover SPI")
public class GridNeverFailoverSpiSelfTest extends GridSpiAbstractTest<NeverFailoverSpi> {
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