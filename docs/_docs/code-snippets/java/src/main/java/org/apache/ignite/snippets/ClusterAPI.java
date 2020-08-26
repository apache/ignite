package org.apache.ignite.snippets;

import java.util.UUID;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterState;
import org.junit.jupiter.api.Test;

public class ClusterAPI {

    @Test
    void activate() {
        //tag::activate[]
        Ignite ignite = Ignition.start();

        ignite.cluster().state(ClusterState.ACTIVE);
        //end::activate[]
        ignite.close();
    }

    @Test
    void changeClusterState() {
        //tag::change-state[]
        Ignite ignite = Ignition.start();

        ignite.cluster().state(ClusterState.ACTIVE_READ_ONLY);
        //end::change-state[]
        ignite.close();
    }

    void changeClusterTag() throws IgniteCheckedException {
        //tag::cluster-tag[]
        Ignite ignite = Ignition.start();

        // get the cluster id
       java.util.UUID clusterId = ignite.cluster().id();
       
       // change the cluster tag
       ignite.cluster().tag("new_tag");

        //end::cluster-tag[]
        ignite.close();
    }

    @Test
    void enableAutoadjustment() {
        //tag::enable-autoadjustment[]

        Ignite ignite = Ignition.start();

        ignite.cluster().baselineAutoAdjustEnabled(true);

        ignite.cluster().baselineAutoAdjustTimeout(30000);

        //end::enable-autoadjustment[]

        //tag::disable-autoadjustment[]
        ignite.cluster().baselineAutoAdjustEnabled(false);
        //end::disable-autoadjustment[]
        ignite.close();
    }

    @Test
    void remoteNodes() {
        // tag::remote-nodes[]
        Ignite ignite = Ignition.ignite();

        IgniteCluster cluster = ignite.cluster();

        // Get compute instance which will only execute
        // over remote nodes, i.e. all the nodes except for this one.
        IgniteCompute compute = ignite.compute(cluster.forRemotes());

        // Broadcast to all remote nodes and print the ID of the node
        // on which this closure is executing.
        compute.broadcast(
                () -> System.out.println("Hello Node: " + ignite.cluster().localNode().id()));
        // end::remote-nodes[]
    }

    @Test
    void example(Ignite ignite) {
        // tag::group-examples[]
        IgniteCluster cluster = ignite.cluster();

        // All nodes on which the cache with name "myCache" is deployed,
        // either in client or server mode.
        ClusterGroup cacheGroup = cluster.forCacheNodes("myCache");

        // All data nodes responsible for caching data for "myCache".
        ClusterGroup dataGroup = cluster.forDataNodes("myCache");

        // All client nodes that can access "myCache".
        ClusterGroup clientGroup = cluster.forClientNodes("myCache");

        // end::group-examples[]
    }

}
