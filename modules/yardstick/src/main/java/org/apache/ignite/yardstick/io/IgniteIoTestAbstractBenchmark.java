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

package org.apache.ignite.yardstick.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

/**
 *
 */
public abstract class IgniteIoTestAbstractBenchmark extends IgniteAbstractBenchmark {
    /** */
    protected final List<ClusterNode> targetNodes = new ArrayList<>();

    /** */
    protected IgniteKernal ignite;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        ignite = (IgniteKernal)ignite();

        ClusterNode loc = ignite().cluster().localNode();

        Collection<ClusterNode> nodes = ignite().cluster().forServers().nodes();

        for (ClusterNode node : nodes) {
            if (!loc.equals(node))
                targetNodes.add(node);
        }

        if (targetNodes.isEmpty())
            throw new IgniteException("Failed to find remote server nodes [nodes=" + nodes + ']');

        BenchmarkUtils.println(cfg, "Initialized target nodes: " + F.nodeIds(targetNodes) + ']');
    }
}
