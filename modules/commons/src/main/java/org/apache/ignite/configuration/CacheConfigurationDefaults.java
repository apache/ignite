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

package org.apache.ignite.configuration;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.PartitionLossPolicy;

/** */
public interface CacheConfigurationDefaults {
    /** Default atomicity mode. */
    public static final CacheAtomicityMode DFLT_CACHE_ATOMICITY_MODE = CacheAtomicityMode.ATOMIC;

    /** Default number of backups. */
    public static final int DFLT_BACKUPS = 0;

    /** Default caching mode. */
    public static final CacheMode DFLT_CACHE_MODE = CacheMode.PARTITIONED;

    /** Default value for eager ttl flag. */
    public static final boolean DFLT_EAGER_TTL = true;

    /**
     * Default lock timeout.
     * @deprecated Default lock timeout configuration property has no effect.
     */
    @Deprecated
    public static final long DFLT_LOCK_TIMEOUT = 0;

    /** Default partition loss policy. */
    public static final PartitionLossPolicy DFLT_PARTITION_LOSS_POLICY = PartitionLossPolicy.IGNORE;

    /** Default value for 'readFromBackup' flag. */
    public static final boolean DFLT_READ_FROM_BACKUP = true;

    /** Default rebalance mode for distributed cache. */
    public static final CacheRebalanceMode DFLT_REBALANCE_MODE = CacheRebalanceMode.ASYNC;

    /** Default value for 'copyOnRead' flag. */
    public static final boolean DFLT_COPY_ON_READ = true;

    /** Default value for 'maxConcurrentAsyncOps'. */
    public static final int DFLT_MAX_CONCURRENT_ASYNC_OPS = 500;

    /** Default maximum number of query iterators that can be stored. */
    public static final int DFLT_MAX_QUERY_ITERATOR_CNT = 1024;

    /** Default number of queries detail metrics to collect. */
    public static final int DFLT_QRY_DETAIL_METRICS_SIZE = 0;

    /** Default query parallelism. */
    public static final int DFLT_QUERY_PARALLELISM = 1;

    /** Default maximum inline size for sql indexes. */
    public static final int DFLT_SQL_INDEX_MAX_INLINE_SIZE = -1;

    /** Maximum number of partitions. */
    public static final int MAX_PARTITIONS_COUNT = 65000;
}
