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

package org.apache.ignite.internal;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Node filter test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridNodeFilterSelfTest extends GridCommonAbstractTest {
    /** Grid instance. */
    private Ignite ignite;

    /** Remote instance. */
    private Ignite rmtIgnite;

    /** */
    public GridNodeFilterSelfTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite = startGrid(1);

        rmtIgnite = startGrid(2);
        startGrid(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopGrid(1);
        stopGrid(2);
        stopGrid(3);

        ignite = null;
        rmtIgnite = null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSynchronousExecute() throws Exception {
        UUID nodeId = ignite.cluster().localNode().id();

        UUID rmtNodeId = rmtIgnite.cluster().localNode().id();

        Collection<ClusterNode> locNodes = ignite.cluster().forNodeId(nodeId).nodes();

        assert locNodes.size() == 1;
        assert locNodes.iterator().next().id().equals(nodeId);

        Collection<ClusterNode> rmtNodes = ignite.cluster().forNodeId(rmtNodeId).nodes();

        assert rmtNodes.size() == 1;
        assert rmtNodes.iterator().next().id().equals(rmtNodeId);
    }
}