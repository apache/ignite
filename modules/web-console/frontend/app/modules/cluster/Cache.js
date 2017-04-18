/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

export default class Cache {
    constructor(cache) {
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

        const m = cache.metrics;

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
