

export default class Cache {
    constructor(cache) {
        this.dynamicDeploymentId = cache.dynamicDeploymentId;

        // Name.
        this.name = cache.name;

        // Mode.
        this.mode = cache.mode;

        // Heap.
        this.size = cache.size;
        this.primarySize = cache.primarySize;
        this.backupSize = _.isNil(cache.backupSize) ? cache.dhtSize - cache.primarySize : cache.backupSize;
        this.nearSize = cache.nearSize;

        const m = cache.metrics;

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
