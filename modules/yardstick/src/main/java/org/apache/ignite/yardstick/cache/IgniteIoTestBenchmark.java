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

package org.apache.ignite.yardstick.cache;

import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class IgniteIoTestBenchmark extends IgniteAbstractBenchmark {
    /** */
    private List<ClusterNode> targetNodes;

    /** */
    private IgniteKernal ignite;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        ignite = (IgniteKernal)ignite();

        targetNodes = new ArrayList<>();

        ClusterNode loc = ignite().cluster().localNode();

        Collection<ClusterNode> nodes = ignite().cluster().forServers().nodes();

        for (ClusterNode node : nodes) {
            if (!loc.equals(node))
                targetNodes.add(node);
        }

        if (targetNodes.isEmpty())
            throw new IgniteException("Failed to find remote server nodes [nodes=" + nodes + ']');

        BenchmarkUtils.println(cfg, "Initialized target nodes: " + targetNodes + ']');
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        ClusterNode node = targetNodes.get(nextRandom(targetNodes.size()));

        ignite.sendIoTest(node, null, false).get();

        return true;
    }
}
