/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
