
import _ from 'lodash';

export default class NodeMetrics {
    constructor(node,metrics) {
        this.id = node.id;

        // Name.
        this.instanceName = node.instanceName;
        this.consistentId = node.consistentId;

        // Mode.
        this.isClient = node.isClient;        

        const m = metrics;

        // Off-heap.
        this.offHeapAllocatedSize = m.offHeapAllocatedSize;
        this.offHeapSize = m.offHeapEntriesCount;
        this.offHeapPrimarySize = m.offHeapPrimaryEntriesCount || 0;
        this.offHeapBackupSize = this.offHeapSize - this.offHeapPrimarySize;

        // Read/write metrics.
        this.hits = m.hits;
        this.misses = m.misses;
        this.reads = m.reads;
        this.writes = m.writes;

        // Transaction metrics.
        this.commits = m.txCommits;
        this.rollbacks = m.txRollbacks;
    }
}
