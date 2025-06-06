

export default class CacheMetrics {
    constructor(cache,metrics) {
        this.dynamicDeploymentId = cache.dynamicDeploymentId;

        // Name.
        this.name = cache.name;

        // Mode.
        this.mode = cache.mode;

        // Memory Usage.
        this.memorySize = cache.memorySize;

        // Heap.
        this.size = cache.size;
        this.primarySize = cache.primarySize;
        this.backupSize = cache.dhtSize - cache.primarySize;
        this.nearSize = cache.nearSize;

        // Off-heap.
        this.offHeapAllocatedSize = cache.offHeapAllocatedSize;
        this.offHeapSize = cache.offHeapEntriesCount;
        this.offHeapPrimarySize = cache.offHeapPrimaryEntriesCount || 0;
        this.offHeapBackupSize = cache.offHeapBackupEntriesCount || 0;

        // Swap.
        this.swapSize = cache.swapSize;
        this.swapKeys = cache.swapKeys;

        const m = metrics;

        // Read/write metrics.
        this.hits = m.hits;
        this.misses = m.misses;
        this.reads = m.reads;
        this.writes = m.writes;

        // Transaction metrics.
        this.commits = m.txCommits;
        this.rollbacks = m.txRollbacks;

        // Admin metrics.
        this.statisticsEnabled = m.statisticsEnabled;
    }
}
