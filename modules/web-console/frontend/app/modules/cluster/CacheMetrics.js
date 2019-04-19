/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

export default class CacheMetrics {
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

        // Admin metrics.
        this.statisticsEnabled = m.statisticsEnabled;
    }
}
