package org.apache.ignite.internal.ducktest.tests.cellular_affinity_test;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 *
 */
public class DistributionChecker extends IgniteAwareApplication {
    /**
     * @param ignite Ignite.
     */
    public DistributionChecker(Ignite ignite) {
        super(ignite);
    }

    /**
     * {@inheritDoc}
     */
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
