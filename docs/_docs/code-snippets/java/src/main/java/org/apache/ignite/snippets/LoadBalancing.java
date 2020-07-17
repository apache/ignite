package org.apache.ignite.snippets;

import java.util.Collections;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.spi.collision.jobstealing.JobStealingCollisionSpi;
import org.apache.ignite.spi.failover.jobstealing.JobStealingFailoverSpi;
import org.apache.ignite.spi.loadbalancing.roundrobin.RoundRobinLoadBalancingSpi;
import org.apache.ignite.spi.loadbalancing.weightedrandom.WeightedRandomLoadBalancingSpi;

public class LoadBalancing {

    void roundRobin() {
        // tag::load-balancing[]
        RoundRobinLoadBalancingSpi spi = new RoundRobinLoadBalancingSpi();
        spi.setPerTask(true);

        IgniteConfiguration cfg = new IgniteConfiguration();
        // these events are required for the per-task mode
        cfg.setIncludeEventTypes(EventType.EVT_TASK_FINISHED, EventType.EVT_TASK_FAILED, EventType.EVT_JOB_MAPPED);

        // Override default load balancing SPI.
        cfg.setLoadBalancingSpi(spi);

        // Start a node.
        Ignite ignite = Ignition.start(cfg);
        // end::load-balancing[]

        ignite.close();
    }

    void weighted() {

        // tag::weighted[]
        WeightedRandomLoadBalancingSpi spi = new WeightedRandomLoadBalancingSpi();

        // Configure SPI to use the weighted random load balancing algorithm.
        spi.setUseWeights(true);

        // Set weight for the local node.
        spi.setNodeWeight(10);

        IgniteConfiguration cfg = new IgniteConfiguration();

        // Override default load balancing SPI.
        cfg.setLoadBalancingSpi(spi);

        // Start a node.
        Ignite ignite = Ignition.start(cfg);
        // end::weighted[]

        ignite.close();
    }

    void jobStealing() {
        //tag::job-stealing[]
        JobStealingCollisionSpi spi = new JobStealingCollisionSpi();

        // Configure number of waiting jobs
        // in the queue for job stealing.
        spi.setWaitJobsThreshold(10);

        // Configure message expire time (in milliseconds).
        spi.setMessageExpireTime(1000);

        // Configure stealing attempts number.
        spi.setMaximumStealingAttempts(10);

        // Configure number of active jobs that are allowed to execute
        // in parallel. This number should usually be equal to the number
        // of threads in the pool (default is 100).
        spi.setActiveJobsThreshold(50);

        // Enable stealing.
        spi.setStealingEnabled(true);

        // Set stealing attribute to steal from/to nodes that have it.
        spi.setStealingAttributes(Collections.singletonMap("node.segment", "foobar"));

        // Enable `JobStealingFailoverSpi`
        JobStealingFailoverSpi failoverSpi = new JobStealingFailoverSpi();

        IgniteConfiguration cfg = new IgniteConfiguration();

        // Override default Collision SPI.
        cfg.setCollisionSpi(spi);

        cfg.setFailoverSpi(failoverSpi);
        //end::job-stealing[]
        Ignition.start(cfg).close();
    }

    public static void main(String[] args) {
        LoadBalancing lb = new LoadBalancing();

        lb.roundRobin();
        lb.weighted();
    }

}
