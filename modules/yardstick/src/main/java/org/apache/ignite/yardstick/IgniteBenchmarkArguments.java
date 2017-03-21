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

package org.apache.ignite.yardstick;

import com.beust.jcommander.Parameter;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import java.util.ArrayList;
import java.util.List;

/**
 * Input arguments for Ignite benchmarks.
 */
@SuppressWarnings({"UnusedDeclaration", "FieldCanBeLocal"})
public class IgniteBenchmarkArguments {
    /** */
    @Parameter(names = {"-nn", "--nodeNumber"}, description = "Node number")
    private int nodes = 1;

    /** */
    @Parameter(names = {"-b", "--backups"}, description = "Backups")
    private int backups;

    /** */
    @Parameter(names = {"-cfg", "--Config"}, description = "Configuration file")
    private String cfg = "config/ignite-localhost-config.xml";

    /** */
    @Parameter(names = {"-ltqf", "--loadTestQueriesFile"}, description = "File with predefined SQL queries")
    private String loadTestQueriesFile = null;

    /** */
    @Parameter(names = {"-sm", "--syncMode"}, description = "Synchronization mode")
    private CacheWriteSynchronizationMode syncMode = CacheWriteSynchronizationMode.PRIMARY_SYNC;

    /** */
    @Parameter(names = {"-cl", "--client"}, description = "Client flag")
    private boolean clientOnly = false;

    /** */
    @Parameter(names = {"-nc", "--nearCache"}, description = "Near cache flag")
    private boolean nearCacheFlag = false;

    /** */
    @Parameter(names = {"-ncs", "--nearCacheSize"}, description = "Near cache size")
    private int nearCacheSize;

    /** */
    @Parameter(names = {"-wom", "--writeOrderMode"}, description = "Write ordering mode")
    private CacheAtomicWriteOrderMode orderMode;

    /** */
    @Parameter(names = {"-txc", "--txConcurrency"}, description = "Transaction concurrency")
    private TransactionConcurrency txConcurrency = TransactionConcurrency.PESSIMISTIC;

    /** */
    @Parameter(names = {"-txi", "--txIsolation"}, description = "Transaction isolation")
    private TransactionIsolation txIsolation = TransactionIsolation.REPEATABLE_READ;

    /** */
    @Parameter(names = {"-ot", "--offheapTiered"}, description = "Tiered offheap")
    private boolean offheapTiered;

    /** */
    @Parameter(names = {"-ov", "--offheapValuesOnly"}, description = "Offheap values only")
    private boolean offheapVals;

    /** */
    @Parameter(names = {"-rtp", "--restPort"}, description = "REST TCP port")
    private int restTcpPort;

    /** */
    @Parameter(names = {"-rth", "--restHost"}, description = "REST TCP host")
    private String restTcpHost;

    /** */
    @Parameter(names = {"-r", "--range"}, description = "Key range")
    public int range = 1_000_000;

    /** */
    @Parameter(names = {"-pa", "--preloadAmount"}, description = "Data pre-loading amount for load tests")
    public int preloadAmount = 500_000;

    /** */
    @Parameter(names = {"-plfreq", "--preloadLogFrequency"}, description = "Interval between printing logs")
    public long preloadLogsInterval = 30_000;

    /** */
    @Parameter(names = {"-j", "--jobs"}, description = "Number of jobs for compute benchmarks")
    private int jobs = 10;

    /** */
    @Parameter(names = {"-cs", "--cacheStore"}, description = "Enable or disable cache store readThrough, writeThrough")
    private boolean storeEnabled;

    /** */
    @Parameter(names = {"-wb", "--writeBehind"}, description = "Enable or disable writeBehind for cache store")
    private boolean writeBehind;

    /** */
    @Parameter(names = {"-bs", "--batchSize"}, description = "Batch size")
    private int batch = 500;

    /** */
    @Parameter(names = {"-col", "--collocated"}, description = "Collocated")
    private boolean collocated;

    /** */
    @Parameter(names = {"-jdbc", "--jdbcUrl"}, description = "JDBC url")
    private String jdbcUrl;

    /** */
    @Parameter(names = {"-rd", "--restartdelay"}, description = "Restart delay in seconds")
    private int restartDelay = 20;

    /** */
    @Parameter(names = {"-rs", "--restartsleep"}, description = "Restart sleep in seconds")
    private int restartSleep = 2;

    /** */
    @Parameter(names = {"-checkingPeriod", "--checkingPeriod"}, description = "Period to check cache consistency in seconds")
    private int cacheConsistencyCheckingPeriod = 2 * 60;

    /** */
    @Parameter(names = {"-kc", "--keysCount"}, description = "Count of keys")
    private int keysCnt = 5;

    /** */
    @Parameter(names = {"-cot", "--cacheOperationTimeout"}, description = "Max timeout for cache operations in seconds")
    private int cacheOpTimeout = 30;

    /** */
    @Parameter(names = {"-kpt", "--keysPerThread"}, description = "Use not intersecting keys in putAll benchmark")
    private boolean keysPerThread;

    /** */
    @Parameter(names = {"-ac", "--additionalCachesNumber"}, description = "Number of additional caches")
    private int additionalCachesNum;

    /** */
    @Parameter(names = {"-acn", "--additionalCachesName"}, description = "Template cache name for additional caches")
    private String additionalCachesName;

    /** */
    @Parameter(names = {"-pp", "--printPartitionStats"}, description = "Print partition statistics")
    private boolean printPartStats;

    /** */
    @Parameter(names = {"-ltops", "--allowedLoadTestOperations"}, variableArity = true, description = "List of enabled load test operations")
    private List<String> allowedLoadTestOps = new ArrayList<>();

    /**
     * @return List of enabled load test operations.
     */
    public List<String> allowedLoadTestOps() {
        return allowedLoadTestOps;
    }

    /**
     * @return If {@code true} when need to print partition statistics.
     */
    public boolean printPartitionStatistics() {
        return printPartStats;
    }

    /**
     * @return JDBC url.
     */
    public String jdbcUrl() {
        return jdbcUrl;
    }

    /**
     * @return Transaction concurrency.
     */
    public TransactionConcurrency txConcurrency() {
        return txConcurrency;
    }

    /**
     * @return Transaction isolation.
     */
    public TransactionIsolation txIsolation() {
        return txIsolation;
    }

    /**
     * @return REST TCP port.
     */
    public int restTcpPort() {
        return restTcpPort;
    }

    /**
     * @return REST TCP host.
     */
    public String restTcpHost() {
        return restTcpHost;
    }

    /**
     * @return Distribution.
     */
    public boolean isClientOnly() {
        return clientOnly;
    }

    /**
     * @return Near cache flag.
     */
    public boolean isNearCache() {
        return nearCacheFlag;
    }

    /**
     * @return Near cache size ({@code 0} for unlimited).
     */
    public int getNearCacheSize() {
        return nearCacheSize;
    }

    /**
     * @return Synchronization.
     */
    public CacheWriteSynchronizationMode syncMode() {
        return syncMode;
    }

    /**
     * @return Cache write ordering mode.
     */
    public CacheAtomicWriteOrderMode orderMode() {
        return orderMode;
    }

    /**
     * @return Backups.
     */
    public int backups() {
        return backups;
    }

    /**
     * @return Offheap tiered.
     */
    public boolean isOffheapTiered() {
        return offheapTiered;
    }

    /**
     * @return Offheap values.
     */
    public boolean isOffheapValues() {
        return offheapVals;
    }

    /**
     * @return {@code True} if any offheap is enabled.
     */
    public boolean isOffHeap() {
        return offheapTiered || offheapVals;
    }

    /**
     * @return Nodes.
     */
    public int nodes() {
        return nodes;
    }

    /**
     * @return Key range, from {@code 0} to this number.
     */
    public int range() {
        return range;
    }

    /**
     * @return Preload key range, from {@code 0} to this number.
     */
    public int preloadAmount() {
        return preloadAmount;
    }

    /**
     * @return Preload log printing interval in seconds.
     */
    public long preloadLogsInterval() {
        return preloadLogsInterval;
    }

    /**
     * @return Configuration file.
     */
    public String configuration() {
        return cfg;
    }

    /**
     * @return File with predefined SQL queries.
     */
    public String loadTestQueriesFile() {
        return loadTestQueriesFile;
    }

    /**
     * @return Number of jobs
     */
    public int jobs() {
        return jobs;
    }

    /**
     * @return {@code True} if enabled readThrough, writeThrough for cache.
     */
    public boolean isStoreEnabled() {
        return storeEnabled;
    }

    /**
     * @return {@code True} if enabled writeBehind for cache store.
     */
    public boolean isWriteBehind() {
        return writeBehind;
    }

    /**
     * @return Batch size.
     */
    public int batch() {
        return batch;
    }

    /**
     * @return Collocated.
     */
    public boolean collocated() {
        return collocated;
    }

    /**
     * @return Delay in second which used in nodes restart algorithm.
     */
    public int restartDelay() {
        return restartDelay;
    }

    /**
     * @return Sleep in second which used in nodes restart algorithm.
     */
    public int restartSleep() {
        return restartSleep;
    }

    /**
     * @return Keys count.
     */
    public int keysCount() {
        return keysCnt;
    }

    /**
     * @return Period in seconds to check cache consistency.
     */
    public int cacheConsistencyCheckingPeriod() {
        return cacheConsistencyCheckingPeriod;
    }

    /**
     * @return Cache operation timeout in milliseconds.
     */
    public int cacheOperationTimeoutMillis() {
        return cacheOpTimeout * 1000;
    }

    /**
     * @return {@code True} if use not intersecting keys in putAll benchmark.
     */
    public boolean keysPerThread() {
        return keysPerThread;
    }

    /**
     * @return Number of additional caches.
     */
    public int additionalCachesNumber() {
        return additionalCachesNum;
    }

    /**
     * @return Name of cache which will be taken as base for additional caches.
     */
    public String additionalCachesName() {
        return additionalCachesName;
    }

    /**
     * @return Description.
     */
    public String description() {
        return "-nn=" + nodes + "-b=" + backups + "-sm=" + syncMode + "-cl=" + clientOnly + "-nc=" + nearCacheFlag +
            (orderMode == null ? "" : "-wom=" + orderMode) + "-txc=" + txConcurrency + "-rd=" + restartDelay +
            "-rs=" + restartSleep;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(IgniteBenchmarkArguments.class, this);
    }
}
