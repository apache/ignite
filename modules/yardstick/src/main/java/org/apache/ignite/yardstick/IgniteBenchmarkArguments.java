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
import com.beust.jcommander.ParametersDelegate;
import java.util.Collections;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.yardstick.cache.IgniteStreamerBenchmark;
import org.apache.ignite.yardstick.upload.UploadBenchmarkArguments;
import org.jetbrains.annotations.Nullable;

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
    @Parameter(names = {"--atomic-mode", "--atomicMode"})
    @Nullable private CacheAtomicityMode atomicMode = null;

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
    @Parameter(names = {"-txc", "--txConcurrency"}, description = "Transaction concurrency")
    private TransactionConcurrency txConcurrency = TransactionConcurrency.PESSIMISTIC;

    /** */
    @Parameter(names = {"-txi", "--txIsolation"}, description = "Transaction isolation")
    private TransactionIsolation txIsolation = TransactionIsolation.REPEATABLE_READ;

    /** */
    @Parameter(names = {"-rtp", "--restPort"}, description = "REST TCP port")
    private int restTcpPort;

    /** */
    @Parameter(names = {"-rth", "--restHost"}, description = "REST TCP host")
    private String restTcpHost;

    /** */
    @Parameter(names = {"-r", "--range"}, description = "Key range")
    @GridToStringInclude
    private int range = 1_000_000;

    /** */
    @Parameter(names = {"-sf", "--scaleFactor"}, description = "Scale factor")
    private int scaleFactor = 1;

    /** */
    @Parameter(names = {"-ntv", "--native"}, description = "Native benchmarking flag")
    private boolean ntv = false;

    /** */
    @Parameter(names = {"-pa", "--preloadAmount"}, description = "Data pre-loading amount for load tests")
    private int preloadAmount = 500_000;

    /** */
    @Parameter(names = {"-pdrm", "--preloadDataRegionMult"}, description = "Data region size multiplier for preload.")
    private int preloadDataRegionMult = 0;

    /** */
    @Parameter(names = {"-ep", "--enablePreload"}, description = "Enable preload flag.")
    private boolean enablePreload = false;

    /** */
    @Parameter(names = {"-plfreq", "--preloadLogFrequency"}, description = "Interval between printing logs")
    private long preloadLogsInterval = 30_000;

    /** */
    @Parameter(names = {"-j", "--jobs"}, description = "Number of jobs for compute benchmarks")
    private int jobs = 10;

    /** */
    @Parameter(names = {"-cs", "--cacheStore"}, description = "Enable or disable cache store readThrough, writeThrough")
    private boolean storeEnabled;

    /** */
    @Parameter(names = {"-cwd", "--cleanWorkDirectory"}, description = "Clean Work Directory")
    private boolean cleanWorkDirectory = false;

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
    @Parameter(names = {"-stripe", "--singleStripe"}, description = "Generate keys belonging to single stripe per node")
    private boolean singleStripe;

    /** */
    @Parameter(names = {"-jdbc", "--jdbcUrl"}, description = "JDBC url")
    private String jdbcUrl;

    /** */
    @Parameter(names = {"-sch", "--schema"}, description = "File with SQL schema definition")
    private String schemaDefinition = null;

    /** */
    @Parameter(names = {"-jdbcDrv", "--jdbcDriver"}, description = "FQN of driver class for JDBC native benchmarks " +
        "(must be on classpath)")
    private String jdbcDriver = null;

    /** */
    @Parameter(names = {"-tempDb", "--temporaryDatabase"}, description = "Whether it's needed to create and drop " +
        "temporary database for JDBC benchmarks dummy data")
    private boolean createTempDatabase = false;

    /** */
    @Parameter(names = {"-dbn", "--databaseName"}, description = "Name of database")
    private String dbn = null;

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
    @Parameter(names = {"-pc", "--partitionedCachesNumber"}, description = "Number of partitioned caches")
    private int partitionedCachesNumber = 1;

    /** */
    @Parameter(names = {"-rc", "--replicatedCachesNumber"}, description = "Number of replicated caches")
    private int replicatedCachesNumber = 1;

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

    /** */
    @Parameter(names = {"-ps", "--pageSize"}, description = "Page size")
    private int pageSize = DataStorageConfiguration.DFLT_PAGE_SIZE;

    /** */
    @Parameter(names = {"-sl", "--stringLength"}, description = "Test string length")
    private int stringLength = 500;

    /** */
    @Parameter(names = {"-wt", "--warningTime"}, description = "Warning time interval for printing log")
    private long warningTime = 500;

    /** */
    @Parameter(names = {"-prb", "--printRollBacks"}, description = "Print rollBacks")
    private boolean printRollBacks;

    /** */
    @Parameter(names = {"-prt", "--partitions"}, description = "Number of cache partitions")
    private int partitions = 10;

    /** */
    @Parameter(names = {"-cg", "--cacheGrp"}, description = "Cache group for caches")
    private String cacheGrp;

    /** */
    @Parameter(names = {"-cc", "--cachesCnt"}, description = "Number of caches to create")
    private int cachesCnt = 1;

    /** */
    @Parameter(names = {"-pds", "--persistentStore"}, description = "Persistent store flag")
    private boolean persistentStoreEnabled;

    /** */
    @Parameter(names = {"-stcp", "--streamerCachesPrefix"}, description = "Cache name prefix for streamer benchmark")
    private String streamerCachesPrefix = "streamer";

    /** */
    @Parameter(names = {"-stci", "--streamerCachesIndex"}, description = "First cache index for streamer benchmark")
    private int streamerCacheIndex;

    /** */
    @Parameter(names = {"-stcc", "--streamerConcCaches"}, description = "Number of concurrently loaded caches for streamer benchmark")
    private int streamerConcurrentCaches = 1;

    /** */
    @Parameter(names = {"-stbs", "--streamerBufSize"}, description = "Data streamer buffer size")
    private int streamerBufSize = IgniteDataStreamer.DFLT_PER_NODE_BUFFER_SIZE;

    /** */
    @Parameter(names = {"-mvcc", "--mvcc"}, description = "Enable MVCC for cache")
    private boolean mvcc;

    /**
     * @return {@code True} if need enable cache mvcc (see {@link CacheConfiguration#isMvccEnabled()}).
     */
    public boolean mvccEnabled() {
        return mvcc;
    }

    /** */
    @Parameter(names = {"-sqlr", "--sqlRange"}, description = "Result set size")
    @GridToStringInclude
    private int sqlRange = 1;

    /** */
    @Parameter(names = {"-clidx", "--clientNodesAfterId"},
        description = "Start client nodes when server ID greater then the parameter value")
    @GridToStringInclude
    private int clientNodesAfterId = -1;

    @ParametersDelegate
    @GridToStringInclude
    public UploadBenchmarkArguments upload = new UploadBenchmarkArguments();

    /** */
    @Parameter(names = {"--mvcc-contention-range", "--mvccContentionRange"},
        description = "Mvcc benchmark specific: " +
            "Size of range of table keys that should be used in query. " +
            "Should be less than 'range'. " +
            "Useful together with 'sqlRange' to control, how often key contentions of sql operations occur.")
    @GridToStringInclude
    public long mvccContentionRange = 10_000;

    /**
     * @return {@code True} if need set {@link DataStorageConfiguration}.
     */
    public boolean persistentStoreEnabled() {
        return persistentStoreEnabled;
    }

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
     * @return JDBC driver.
     */
    public String jdbcDriver() {
        return jdbcDriver;
    }

    /**
     * @return schema definition.
     */
    public String schemaDefinition() {
        return schemaDefinition;
    }

    /**
     * @return flag for creation temporary database.
     */
    public boolean createTempDatabase() {
        return createTempDatabase;
    }

    /**
     * @return existing database name defined in property file.
     */
    public String dbn() {
        return dbn;
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

    /** With what cache atomicity mode to create tables. */
    @Nullable public CacheAtomicityMode atomicMode(){
        return atomicMode;
    }

    /**
     * @return Backups.
     */
    public int backups() {
        return backups;
    }

    /**
     * @return {@code True} if flag for native benchmarking is set.
     */
    public boolean isNative() {
        return ntv;
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

    public void setRange(int newVal) {
        range = newVal;
    }

    /**
     * @return Scale factor.
     */
    public int scaleFactor() {
        return scaleFactor;
    }

    /**
     * @return Preload key range, from {@code 0} to this number.
     */
    public int preloadAmount() {
        return preloadAmount;
    }

    /**
     * @return Preload data region multiplier.
     */
    public int preloadDataRegionMult() {
        return preloadDataRegionMult;
    }

    /**
     * @return Reset range for preload flag.
     */
    public boolean enablePreload() {
        return enablePreload;
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
     * @return Generate keys for single stripe per node.
     */
    public boolean singleStripe() {
        return singleStripe;
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
     * @return Page size in bytes.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * @return Test string length.
     */
    public int getStringLength() {
        return stringLength;
    }

    /**
     * @return Warning time interval.
     */
    public long getWarningTime() {
        return warningTime;
    }

    /**
     * @return Flag for printing rollbacks.
     */
    public boolean printRollBacks() {
        return printRollBacks;
    }

    /**
     * @return Number of partitioned caches.
     */
    public int partitionedCachesNumber() {
        return partitionedCachesNumber;
    }

    /**
     * @return Number of replicated caches.
     */
    public int replicatedCachesNumber() {
        return replicatedCachesNumber;
    }

    /**
     * @return Number of cache partitions.
     */
    public int partitions() {
        return partitions;
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
     * @return Flag for cleaning working directory.
     */
    public boolean cleanWorkDirectory() {
        return cleanWorkDirectory;
    }

    /**
     * @return Name of cache group to be set for caches.
     */
    @Nullable public String cacheGroup() {
        return cacheGrp;
    }

    /**
     * @return Number of caches to create.
     */
    public int cachesCount() {
        return cachesCnt;
    }

    /**
     * @return Description.
     */
    public String description() {
        return "-nn=" + nodes + "-b=" + backups + "-sm=" + syncMode + "-cl=" + clientOnly + "-nc=" + nearCacheFlag +
            "-txc=" + txConcurrency + "-rd=" + restartDelay + "-rs=" + restartSleep;
    }

    /**
     * @return Cache name prefix for caches to be used in {@link IgniteStreamerBenchmark}.
     */
    public String streamerCachesPrefix() {
        return streamerCachesPrefix;
    }

    /**
     * @return First cache index for {@link IgniteStreamerBenchmark}.
     */
    public int streamerCacheIndex() {
        return streamerCacheIndex;
    }

    /**
     * @return Number of concurrently loaded caches for {@link IgniteStreamerBenchmark}.
     */
    public int streamerConcurrentCaches() {
        return streamerConcurrentCaches;
    }

    /**
     * @return Streamer buffer size {@link IgniteStreamerBenchmark} (see {@link IgniteDataStreamer#perNodeBufferSize()}.
     */
    public int streamerBufferSize() {
        return streamerBufSize;
    }

    /**
     * @return Result set size.
     */
    public int sqlRange() {
        return sqlRange;
    }

    /**
     * @return Result set size.
     */
    public int clientNodesAfterId() {
        return clientNodesAfterId;
    }

    /**
     * @return Mvcc contention range.
     */
    public long mvccContentionRange() {
        return mvccContentionRange;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(IgniteBenchmarkArguments.class, this);
    }
}
