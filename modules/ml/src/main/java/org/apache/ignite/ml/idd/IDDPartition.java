package org.apache.ignite.ml.idd;

public class IDDPartition<K, V, D, L> {

    private final IDDPartitionDistributedSegment<K, V, D, L> distributedSegment;

    private final L localSegment;

    public IDDPartition(IDDPartitionDistributedSegment<K, V, D, L> distributedSegment, L localSegment) {
        this.distributedSegment = distributedSegment;
        this.localSegment = localSegment;
    }

    public IDDPartitionDistributedSegment<K, V, D, L> getDistributedSegment() {
        return distributedSegment;
    }

    public L getLocalSegment() {
        return localSegment;
    }
}
