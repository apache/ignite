package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;

public class ClusterGroups {

    void remoteNodes() {
        // tag::remote-nodes[]
        Ignite ignite = Ignition.ignite();

        IgniteCluster cluster = ignite.cluster();

        // Get compute instance which will only execute
        // over remote nodes, i.e. all the nodes except for this one.
        IgniteCompute compute = ignite.compute(cluster.forRemotes());

        // Broadcast to all remote nodes and print the ID of the node
        // on which this closure is executing.
        compute.broadcast(() -> System.out.println("Hello Node: " + ignite.cluster().localNode().id()));
        // end::remote-nodes[]
    }

    void example(Ignite ignite) {
        // tag::examples[]
        IgniteCluster cluster = ignite.cluster();

        // All nodes on which the cache with name "myCache" is deployed,
        // either in client or server mode.
        ClusterGroup cacheGroup = cluster.forCacheNodes("myCache");

        // All data nodes responsible for caching data for "myCache".
        ClusterGroup dataGroup = cluster.forDataNodes("myCache");

        // All client nodes that can access "myCache".
        ClusterGroup clientGroup = cluster.forClientNodes("myCache");

        // end::examples[]
    }
}
