package org.apache.ignite.spi.discovery.datacenter;

/**
 *  Data center resolver enables to supply Data Center ID to a starting node.
 *  It is intended to use only for clusters spanning multiple data centers. For a single DC deployment there is no need
 *  to provide data center ID.
 *  <p/>
 *  Data Center ID is a human-readable string that should be the same for all nodes in a particular data center.
 *  It enables internal components to optimize their operations in an environment with multiple data centers
 *  when network latency and throughput between nodes vary depending on nodes' data centers.
 */
public interface DataCenterResolver {
    /**
     * Returns data center ID based on node configuration or environment, depending on implementation.
     *
     * @return Data center ID.
     */
    public String resolveDataCenterId();
}
