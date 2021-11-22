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

package org.apache.ignite.internal.ducktest.tests.cellular_affinity_test;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 *
 */
public class DistributionChecker extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) {
        String cacheName = jsonNode.get("cacheName").asText();
        String attr = jsonNode.get("attr").asText();
        int nodesPerCell = jsonNode.get("nodesPerCell").intValue();

        assert ignite.cluster().forServers().nodes().size() > nodesPerCell : "Cluster should contain more than one cell";

        for (int i = 0; i < 10_000; i++) {
            Collection<ClusterNode> nodes = ignite.affinity(cacheName).mapKeyToPrimaryAndBackups(i);

            Map<Object, Long> stat = nodes.stream().collect(
                Collectors.groupingBy(n -> n.attributes().get(attr), Collectors.counting()));

            log.info("Checking [key=" + i + ", stat=" + stat + "]");

            assert 1 == stat.keySet().size() : "Partition should be located on nodes from only one cell [stat=" + stat + "]";

            assert nodesPerCell == stat.values().iterator().next() :
                "Partition should be located on all nodes of the cell [stat=" + stat + "]";
        }

        markSyncExecutionComplete();
    }
}
