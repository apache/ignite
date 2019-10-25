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

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * {@code IGFS} configuration. More than one file system can be configured within grid.
 * {@code IGFS} configuration is provided via {@link IgniteConfiguration#getFileSystemConfiguration()}
 * method.
 */
public class FileSystemConfiguration {
    /** Default file system user name. */
    public static final String DFLT_USER_NAME = System.getProperty("user.name", "anonymous");

    /** Default fragmentizer throttling block length. */
    public static final long DFLT_FRAGMENTIZER_THROTTLING_BLOCK_LENGTH = 16L * 1024 * 1024;

    /** Default fragmentizer throttling delay. */
    public static final long DFLT_FRAGMENTIZER_THROTTLING_DELAY = 200;

    /** Default fragmentizer concurrent files. */
    public static final int DFLT_FRAGMENTIZER_CONCURRENT_FILES = 0;

    /** Fragmentizer enabled property. */
    public static final boolean DFLT_FRAGMENTIZER_ENABLED = true;

    /** Default batch size for logging. */
    public static final int DFLT_IGFS_LOG_BATCH_SIZE = 100;

    /** Default {@code IGFS} log directory. */
    public static final String DFLT_IGFS_LOG_DIR = "work/igfs/log";

    /** Default per node buffer size. */
    public static final int DFLT_PER_NODE_BATCH_SIZE = 512;

    /** Default number of per node parallel operations. */
    public static final int DFLT_PER_NODE_PARALLEL_BATCH_CNT = 16;

    /** Default IGFS mode. */
    public static final IgfsMode DFLT_MODE = IgfsMode.DUAL_ASYNC;

    /** Default file's data block size (bytes). */
    public static final int DFLT_BLOCK_SIZE = 1 << 16;

    /** Default read/write buffers size (bytes). */
    public static final int DFLT_BUF_SIZE = 1 << 16;

    /** Default management port. */
    public static final int DFLT_MGMT_PORT = 11400;

    /** Default IPC endpoint enabled flag. */
    public static final boolean DFLT_IPC_ENDPOINT_ENABLED = true;

    /** Default value of metadata co-location flag. */
    public static final boolean DFLT_COLOCATE_META = true;

    /** Default value of relaxed consistency flag. */
    public static final boolean DFLT_RELAXED_CONSISTENCY = true;

    /** Default value of update file length on flush flag. */
    public static final boolean DFLT_UPDATE_FILE_LEN_ON_FLUSH = false;

    /** IGFS instance name. */
    private String name;

    /** File's data block size (bytes). */
    private int blockSize = DFLT_BLOCK_SIZE;

    /** The number of pre-fetched blocks if specific file's chunk is requested. */
    private int prefetchBlocks;

    /** Amount of sequential block reads before prefetch is triggered. */
    private int seqReadsBeforePrefetch;

    /** Read/write buffers size for stream operations (bytes). */
    private int bufSize = DFLT_BUF_SIZE;

    /** Per node buffer size. */
    private int perNodeBatchSize = DFLT_PER_NODE_BATCH_SIZE;

    /** Per node parallel operations. */
    private int perNodeParallelBatchCnt = DFLT_PER_NODE_PARALLEL_BATCH_CNT;

    /** IPC endpoint configuration. */
    private IgfsIpcEndpointConfiguration ipcEndpointCfg;

    /** IPC endpoint enabled flag. */
    private boolean ipcEndpointEnabled = DFLT_IPC_ENDPOINT_ENABLED;

    /** Management port. */
    private int mgmtPort = DFLT_MGMT_PORT;

    /** Secondary file system */
    private IgfsSecondaryFileSystem secondaryFs;

    /** IGFS mode. */
    private IgfsMode dfltMode = DFLT_MODE;

    /** Fragmentizer throttling block length. */
    private long fragmentizerThrottlingBlockLen = DFLT_FRAGMENTIZER_THROTTLING_BLOCK_LENGTH;

    /** Fragmentizer throttling delay. */
    private long fragmentizerThrottlingDelay = DFLT_FRAGMENTIZER_THROTTLING_DELAY;

    /** Fragmentizer concurrent files. */
    private int fragmentizerConcurrentFiles = DFLT_FRAGMENTIZER_CONCURRENT_FILES;

    /** Fragmentizer enabled flag. */
    private boolean fragmentizerEnabled = DFLT_FRAGMENTIZER_ENABLED;

    /** Path modes. */
    private Map<String, IgfsMode> pathModes;

    /** Maximum range length. */
    private long maxTaskRangeLen;

    /** Metadata co-location flag. */
    private boolean colocateMeta = DFLT_COLOCATE_META;

    /** Relaxed consistency flag. */
    private boolean relaxedConsistency = DFLT_RELAXED_CONSISTENCY;

    /** Update file length on flush flag. */
    private boolean updateFileLenOnFlush = DFLT_UPDATE_FILE_LEN_ON_FLUSH;

    /** Meta cache config. */
    private CacheConfiguration metaCacheCfg;

    /** Data cache config. */
    private CacheConfiguration dataCacheCfg;

    /**
     * Constructs default configuration.
     */
    public FileSystemConfiguration() {
        // No-op.
    }

    /**
     * Constructs the copy of the configuration.
     *
     * @param cfg Configuration to copy.
     */
    public FileSystemConfiguration(FileSystemConfiguration cfg) {
        assert cfg != null;

        /*
         * Must preserve alphabetical order!
         */
        blockSize = cfg.getBlockSize();
        bufSize = cfg.getBufferSize();
        colocateMeta = cfg.isColocateMetadata();
        dataCacheCfg = cfg.getDataCacheConfiguration();
        dfltMode = cfg.getDefaultMode();
        fragmentizerConcurrentFiles = cfg.getFragmentizerConcurrentFiles();
        fragmentizerEnabled = cfg.isFragmentizerEnabled();
        fragmentizerThrottlingBlockLen = cfg.getFragmentizerThrottlingBlockLength();
        fragmentizerThrottlingDelay = cfg.getFragmentizerThrottlingDelay();
        secondaryFs = cfg.getSecondaryFileSystem();
        ipcEndpointCfg = cfg.getIpcEndpointConfiguration();
        ipcEndpointEnabled = cfg.isIpcEndpointEnabled();
        maxTaskRangeLen = cfg.getMaximumTaskRangeLength();
        metaCacheCfg = cfg.getMetaCacheConfiguration();
        mgmtPort = cfg.getManagementPort();
        name = cfg.getName();
        pathModes = cfg.getPathModes();
        perNodeBatchSize = cfg.getPerNodeBatchSize();
        perNodeParallelBatchCnt = cfg.getPerNodeParallelBatchCount();
        prefetchBlocks = cfg.getPrefetchBlocks();
        relaxedConsistency = cfg.isRelaxedConsistency();
        seqReadsBeforePrefetch = cfg.getSequentialReadsBeforePrefetch();
        updateFileLenOnFlush = cfg.isUpdateFileLengthOnFlush();
    }

    /**
     * Gets IGFS instance name.
     *
     * @return IGFS instance name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets IGFS instance name.
     *
     * @param name IGFS instance name.
     * @return {@code this} for chaining.
     */
    public FileSystemConfiguration setName(String name) {
        if (name == null)
            throw new IllegalArgumentException("IGFS name cannot be null");

        this.name = name;

        return this;
    }

    /**
     * Cache config to store IGFS meta information.
     *
     * @return Cache configuration object.
     */
    @Nullable public CacheConfiguration getMetaCacheConfiguration() {
        return metaCacheCfg;
    }

    /**
     * Cache config to store IGFS meta information. If {@code null}, then default config for
     * meta-cache will be used.
     *
     * Default configuration for the meta cache is:
     * <ul>
     *     <li>atomicityMode = TRANSACTIONAL</li>
     *     <li>cacheMode = PARTITIONED</li>
     *     <li>backups = 1</li>
     * </ul>
     *
     * @param metaCacheCfg Cache configuration object.
     * @return {@code this} for chaining.
     */
    public FileSystemConfiguration setMetaCacheConfiguration(CacheConfiguration metaCacheCfg) {
        this.metaCacheCfg = metaCacheCfg;

        return this;
    }

    /**
     * Cache config to store IGFS data.
     *
     * @return Cache configuration object.
     */
    @Nullable public CacheConfiguration getDataCacheConfiguration() {
        return dataCacheCfg;
    }

    /**
     * Cache config to store IGFS data. If {@code null}, then default config for
     * data cache will be used.
     *
     * Default configuration for the data cache is:
     * <ul>
     *     <<li>atomicityMode = TRANSACTIONAL</li>
     *     <li>cacheMode = PARTITIONED</li>
     *     <li>backups = 0</li>
     * </ul>
     *
     * @param dataCacheCfg Cache configuration object.
     * @return {@code this} for chaining.
     */
    public FileSystemConfiguration setDataCacheConfiguration(CacheConfiguration dataCacheCfg) {
        this.dataCacheCfg = dataCacheCfg;

        return this;
    }

    /**
     * Get file's data block size.
     *
     * @return File's data block size.
     */
    public int getBlockSize() {
        return blockSize;
    }

    /**
     * Sets file's data block size.
     *
     * @param blockSize File's data block size (bytes) or {@code 0} to reset default value.
     * @return {@code this} for chaining.
     */
    public FileSystemConfiguration setBlockSize(int blockSize) {
        A.ensure(blockSize >= 0, "blockSize >= 0");

        this.blockSize = blockSize == 0 ? DFLT_BLOCK_SIZE : blockSize;

        return this;
    }

    /**
     * Get number of pre-fetched blocks if specific file's chunk is requested.
     *
     * @return The number of pre-fetched blocks.
     */
    public int getPrefetchBlocks() {
        return prefetchBlocks;
    }

    /**
     * Sets the number of pre-fetched blocks if specific file's chunk is requested.
     *
     * @param prefetchBlocks New number of pre-fetched blocks.
     * @return {@code this} for chaining.
     */
    public FileSystemConfiguration setPrefetchBlocks(int prefetchBlocks) {
        A.ensure(prefetchBlocks >= 0, "prefetchBlocks >= 0");

        this.prefetchBlocks = prefetchBlocks;

        return this;
    }

    /**
     * Get amount of sequential block reads before prefetch is triggered. The
     * higher this value, the longer IGFS will wait before starting to prefetch
     * values ahead of time. Depending on the use case, this can either help
     * or hurt performance.
     * <p>
     * Default is {@code 0} which means that pre-fetching will start right away.
     * <h1 class="header">Integration With Hadoop</h1>
     * This parameter can be also overridden for individual Hadoop MapReduce tasks by passing
     * {@code fs.igfs.[name].open.sequential_reads_before_prefetch} configuration property directly to Hadoop
     * MapReduce task.
     * <p>
     * <b>NOTE:</b> Integration with Hadoop is available only in {@code In-Memory Accelerator For Hadoop} edition.
     *
     * @return Amount of sequential block reads.
     */
    public int getSequentialReadsBeforePrefetch() {
        return seqReadsBeforePrefetch;
    }

    /**
     * Sets amount of sequential block reads before prefetch is triggered. The
     * higher this value, the longer IGFS will wait before starting to prefetch
     * values ahead of time. Depending on the use case, this can either help
     * or hurt performance.
     * <p>
     * Default is {@code 0} which means that pre-fetching will start right away.
     * <h1 class="header">Integration With Hadoop</h1>
     * This parameter can be also overridden for individual Hadoop MapReduce tasks by passing
     * {@code fs.igfs.[name].open.sequential_reads_before_prefetch} configuration property directly to Hadoop
     * MapReduce task.
     * <p>
     * <b>NOTE:</b> Integration with Hadoop is available only in {@code In-Memory Accelerator For Hadoop} edition.
     *
     * @param seqReadsBeforePrefetch Amount of sequential block reads before prefetch is triggered.
     * @return {@code this} for chaining.
     */
    public FileSystemConfiguration setSequentialReadsBeforePrefetch(int seqReadsBeforePrefetch) {
        A.ensure(seqReadsBeforePrefetch >= 0, "seqReadsBeforePrefetch >= 0");

        this.seqReadsBeforePrefetch = seqReadsBeforePrefetch;

        return this;
    }

    /**
     * Get read/write buffer size for {@code IGFS} stream operations in bytes.
     *
     * @return Read/write buffers size (bytes).
     */
    public int getBufferSize() {
        return bufSize;
    }

    /**
     * Sets read/write buffers size for {@code IGFS} stream operations (bytes).
     *
     * @param bufSize Read/write buffers size for stream operations (bytes) or {@code 0} to reset default value.
     * @return {@code this} for chaining.
     */
    public FileSystemConfiguration setBufferSize(int bufSize) {
        A.ensure(bufSize >= 0, "bufSize >= 0");

        this.bufSize = bufSize == 0 ? DFLT_BUF_SIZE : bufSize;

        return this;
    }

    /**
     * Gets number of file blocks buffered on local node before sending batch to remote node.
     *
     * @return Per node buffer size.
     */
    public int getPerNodeBatchSize() {
        return perNodeBatchSize;
    }

    /**
     * Sets number of file blocks collected on local node before sending batch to remote node.
     *
     * @param perNodeBatchSize Per node buffer size.
     * @return {@code this} for chaining.
     */
    public FileSystemConfiguration setPerNodeBatchSize(int perNodeBatchSize) {
        this.perNodeBatchSize = perNodeBatchSize;

        return this;
    }

    /**
     * Gets number of batches that can be concurrently sent to remote node.
     *
     * @return Number of batches for each node.
     */
    public int getPerNodeParallelBatchCount() {
        return perNodeParallelBatchCnt;
    }

    /**
     * Sets number of file block batches that can be concurrently sent to remote node.
     *
     * @param perNodeParallelBatchCnt Per node parallel load operations.
     * @return {@code this} for chaining.
     */
    public FileSystemConfiguration setPerNodeParallelBatchCount(int perNodeParallelBatchCnt) {
        this.perNodeParallelBatchCnt = perNodeParallelBatchCnt;

        return this;
    }

    /**
     * Gets IPC endpoint configuration.
     * <p>
     * Endpoint is needed for communication between IGFS and {@code IgniteHadoopFileSystem} shipped with <b>Ignite
     * Hadoop Accelerator</b>.
     *
     * @return IPC endpoint configuration.
     */
    @Nullable public IgfsIpcEndpointConfiguration getIpcEndpointConfiguration() {
        return ipcEndpointCfg;
    }

    /**
     * Sets IPC endpoint configuration.
     * <p>
     * Endpoint is needed for communication between IGFS and {@code IgniteHadoopFileSystem} shipped with <b>Ignite
     * Hadoop Accelerator</b>.
     *
     * @param ipcEndpointCfg IPC endpoint configuration.
     * @return {@code this} for chaining.
     */
    public FileSystemConfiguration setIpcEndpointConfiguration(@Nullable IgfsIpcEndpointConfiguration ipcEndpointCfg) {
        this.ipcEndpointCfg = ipcEndpointCfg;

        return this;
    }

    /**
     * Get IPC endpoint enabled flag. In case it is set to {@code true} endpoint will be created and bound to specific
     * port. Otherwise endpoint will not be created. Default value is {@link #DFLT_IPC_ENDPOINT_ENABLED}.
     * <p>
     * Endpoint is needed for communication between IGFS and {@code IgniteHadoopFileSystem} shipped with <b>Ignite
     * Hadoop Accelerator</b>.
     *
     * @return {@code True} in case endpoint is enabled.
     */
    public boolean isIpcEndpointEnabled() {
        return ipcEndpointEnabled;
    }

    /**
     * Set IPC endpoint enabled flag. See {@link #isIpcEndpointEnabled()}.
     * <p>
     * Endpoint is needed for communication between IGFS and {@code IgniteHadoopFileSystem} shipped with <b>Ignite
     * Hadoop Accelerator</b>.
     *
     * @param ipcEndpointEnabled IPC endpoint enabled flag.
     * @return {@code this} for chaining.
     */
    public FileSystemConfiguration setIpcEndpointEnabled(boolean ipcEndpointEnabled) {
        this.ipcEndpointEnabled = ipcEndpointEnabled;

        return this;
    }

    /**
     * Gets port number for management endpoint. All IGFS nodes should have this port open
     * for Visor Management Console to work with IGFS.
     * <p>
     * Default value is {@link #DFLT_MGMT_PORT}
     *
     * @return Port number or {@code -1} if management endpoint should be disabled.
     */
    public int getManagementPort() {
        return mgmtPort;
    }

    /**
     * Sets management endpoint port.
     *
     * @param mgmtPort port number or {@code -1} to disable management endpoint.
     * @return {@code this} for chaining.
     */
    public FileSystemConfiguration setManagementPort(int mgmtPort) {
        this.mgmtPort = mgmtPort;

        return this;
    }

    /**
     * Gets mode to specify how {@code IGFS} interacts with Hadoop file system, like {@code HDFS}.
     * Secondary Hadoop file system is provided for pass-through, write-through, and read-through
     * purposes.
     * <p>
     * Default mode is {@link org.apache.ignite.igfs.IgfsMode#DUAL_ASYNC}. If secondary Hadoop file system is
     * not configured, this mode will work just like {@link org.apache.ignite.igfs.IgfsMode#PRIMARY} mode.
     *
     * @return Mode to specify how IGFS interacts with secondary HDFS file system.
     */
    public IgfsMode getDefaultMode() {
        return dfltMode;
    }

    /**
     * Sets {@code IGFS} mode to specify how it should interact with secondary
     * Hadoop file system, like {@code HDFS}. Secondary Hadoop file system is provided
     * for pass-through, write-through, and read-through purposes.
     *
     * @param dfltMode {@code IGFS} mode.
     * @return {@code this} for chaining.
     */
    public FileSystemConfiguration setDefaultMode(IgfsMode dfltMode) {
        this.dfltMode = dfltMode;

        return this;
    }

    /**
     * Gets the secondary file system. Secondary file system is provided for pass-through, write-through,
     * and read-through purposes.
     *
     * @return Secondary file system.
     */
    public IgfsSecondaryFileSystem getSecondaryFileSystem() {
        return secondaryFs;
    }

    /**
     * Sets the secondary file system. Secondary file system is provided for pass-through, write-through,
     * and read-through purposes.
     *
     * @param fileSystem Secondary file system.
     * @return {@code this} for chaining.
     */
    public FileSystemConfiguration setSecondaryFileSystem(IgfsSecondaryFileSystem fileSystem) {
        secondaryFs = fileSystem;

        return this;
    }

    /**
     * Gets map of path prefixes to {@code IGFS} modes used for them.
     * <p>
     * If path doesn't correspond to any specified prefix or mappings are not provided, then
     * {@link #getDefaultMode()} is used.
     *
     * @return Map of paths to {@code IGFS} modes.
     */
    @Nullable public Map<String, IgfsMode> getPathModes() {
        return pathModes;
    }

    /**
     * Sets map of path prefixes to {@code IGFS} modes used for them.
     * <p>
     * If path doesn't correspond to any specified prefix or mappings are not provided, then
     * {@link #getDefaultMode()} is used.
     *
     * @param pathModes Map of paths to {@code IGFS} modes.
     * @return {@code this} for chaining.
     */
    public FileSystemConfiguration setPathModes(Map<String, IgfsMode> pathModes) {
        this.pathModes = pathModes;

        return this;
    }

    /**
     * Gets the length of file chunk to send before delaying the fragmentizer.
     *
     * @return File chunk length in bytes.
     */
    public long getFragmentizerThrottlingBlockLength() {
        return fragmentizerThrottlingBlockLen;
    }

    /**
     * Sets length of file chunk to transmit before throttling is delayed.
     *
     * @param fragmentizerThrottlingBlockLen Block length in bytes.
     * @return {@code this} for chaining.
     */
    public FileSystemConfiguration setFragmentizerThrottlingBlockLength(long fragmentizerThrottlingBlockLen) {
        this.fragmentizerThrottlingBlockLen = fragmentizerThrottlingBlockLen;

        return this;
    }

    /**
     * Gets throttle delay for fragmentizer.
     *
     * @return Throttle delay in milliseconds.
     */
    public long getFragmentizerThrottlingDelay() {
        return fragmentizerThrottlingDelay;
    }

    /**
     * Sets delay in milliseconds for which fragmentizer is paused.
     *
     * @param fragmentizerThrottlingDelay Delay in milliseconds.
     * @return {@code this} for chaining.
     */
    public FileSystemConfiguration setFragmentizerThrottlingDelay(long fragmentizerThrottlingDelay) {
        this.fragmentizerThrottlingDelay = fragmentizerThrottlingDelay;

        return this;
    }

    /**
     * Gets number of files that can be processed by fragmentizer concurrently.
     *
     * @return Number of files to process concurrently.
     */
    public int getFragmentizerConcurrentFiles() {
        return fragmentizerConcurrentFiles;
    }

    /**
     * Sets number of files to process concurrently by fragmentizer.
     *
     * @param fragmentizerConcurrentFiles Number of files to process concurrently.
     * @return {@code this} for chaining.
     */
    public FileSystemConfiguration setFragmentizerConcurrentFiles(int fragmentizerConcurrentFiles) {
        this.fragmentizerConcurrentFiles = fragmentizerConcurrentFiles;

        return this;
    }

    /**
     * Gets flag indicating whether IGFS fragmentizer is enabled. If fragmentizer is disabled, files will be
     * written in distributed fashion.
     *
     * @return Flag indicating whether fragmentizer is enabled.
     */
    public boolean isFragmentizerEnabled() {
        return fragmentizerEnabled;
    }

    /**
     * Sets property indicating whether fragmentizer is enabled.
     *
     * @param fragmentizerEnabled {@code True} if fragmentizer is enabled.
     * @return {@code this} for chaining.
     */
    public FileSystemConfiguration setFragmentizerEnabled(boolean fragmentizerEnabled) {
        this.fragmentizerEnabled = fragmentizerEnabled;

        return this;
    }

    /**
     * Get maximum default range size of a file being split during IGFS task execution. When IGFS task is about to
     * be executed, it requests file block locations first. Each location is defined as {@link org.apache.ignite.igfs.mapreduce.IgfsFileRange} which
     * has length. In case this parameter is set to positive value, then IGFS will split single file range into smaller
     * ranges with length not greater that this parameter. The only exception to this case is when maximum task range
     * length is smaller than file block size. In this case maximum task range size will be overridden and set to file
     * block size.
     * <p>
     * Note that this parameter is applied when task is split into jobs before {@link org.apache.ignite.igfs.mapreduce.IgfsRecordResolver} is
     * applied. Therefore, final file ranges being assigned to particular jobs could be greater than value of this
     * parameter depending on file data layout and selected resolver type.
     * <p>
     * Setting this parameter might be useful when file is highly colocated and have very long consequent data chunks
     * so that task execution suffers from insufficient parallelism. E.g., in case you have one IGFS node in topology
     * and want to process 1Gb file, then only single range of length 1Gb will be returned. This will result in
     * a single job which will be processed in one thread. But in case you provide this configuration parameter and set
     * maximum range length to 16Mb, then 64 ranges will be returned resulting in 64 jobs which could be executed in
     * parallel.
     * <p>
     * Note that some {@code IgniteFs.execute()} methods can override value of this parameter.
     * <p>
     * In case value of this parameter is set to {@code 0} or negative value, it is simply ignored. Default value is
     * {@code 0}.
     *
     * @return Maximum range size of a file being split during IGFS task execution.
     */
    public long getMaximumTaskRangeLength() {
        return maxTaskRangeLen;
    }

    /**
     * Set maximum default range size of a file being split during IGFS task execution.
     * See {@link #getMaximumTaskRangeLength()} for more details.
     *
     * @param maxTaskRangeLen Set maximum default range size of a file being split during IGFS task execution.
     * @return {@code this} for chaining.
     */
    public FileSystemConfiguration setMaximumTaskRangeLength(long maxTaskRangeLen) {
        this.maxTaskRangeLen = maxTaskRangeLen;

        return this;
    }

    /**
     * Get whether to co-locate metadata on a single node.
     * <p>
     * Normally Ignite spread ownership of particular keys among all cache nodes. Transaction with keys owned by
     * different nodes will produce more network traffic and will require more time to complete comparing to
     * transaction with keys owned only by a single node.
     * <p>
     * IGFS stores information about file system structure (metadata) inside a transactional cache configured through
     * {@link #getMetaCacheConfiguration()} property. Metadata updates caused by operations on IGFS usually require
     * several internal keys to be updated. As IGFS metadata cache usually operates
     * in {@link CacheMode#REPLICATED} mode, meaning that all nodes have all metadata locally, it makes sense to give
     * a hint to Ignite to co-locate ownership of all metadata keys on a single node.
     * This will decrease amount of network trips required to update metadata and hence could improve performance.
     * <p>
     * This property should be disabled if you see excessive CPU and network load on a single node, which
     * degrades performance and cannot be explained by business logic of your application.
     * <p>
     * This settings is only used if metadata cache is configured in {@code CacheMode#REPLICATED} mode. Otherwise it
     * is ignored.
     * <p>
     * Defaults to {@link #DFLT_COLOCATE_META}.
     *
     * @return {@code True} if metadata co-location is enabled.
     */
    public boolean isColocateMetadata() {
        return colocateMeta;
    }

    /**
     * Set metadata co-location flag.
     * <p>
     * See {@link #isColocateMetadata()} for more information.
     *
     * @param colocateMeta Whether metadata co-location is enabled.
     * @return {@code this} for chaining.
     */
    public FileSystemConfiguration setColocateMetadata(boolean colocateMeta) {
        this.colocateMeta = colocateMeta;

        return this;
    }

    /**
     * Get relaxed consistency flag.
     * <p>
     * Concurrent file system operations might conflict with each other. E.g. {@code move("/a1/a2", "/b")} and
     * {@code move("/b", "/a1")}. Hence, it is necessary to atomically verify that participating paths are still
     * on their places to keep file system in consistent state in such cases. These checks are expensive in
     * distributed environment.
     * <p>
     * Real applications, e.g. Hadoop jobs, rarely produce conflicting operations. So additional checks could be
     * skipped in these scenarios without any negative effect on file system integrity. It significantly increases
     * performance of file system operations.
     * <p>
     * If value of this flag is {@code true}, IGFS will skip expensive consistency checks. It is recommended to set
     * this flag to {@code false} if your application has conflicting operations, or you do not how exactly users will
     * use your system.
     * <p>
     * This property affects only {@link IgfsMode#PRIMARY} paths.
     * <p>
     * Defaults to {@link #DFLT_RELAXED_CONSISTENCY}.
     *
     * @return {@code True} if relaxed consistency is enabled.
     */
    public boolean isRelaxedConsistency() {
        return relaxedConsistency;
    }

    /**
     * Set relaxed consistency flag.
     * <p>
     * See {@link #isColocateMetadata()} for more information.
     *
     * @param relaxedConsistency Whether to use relaxed consistency optimization.
     * @return {@code this} for chaining.
     */
    public FileSystemConfiguration setRelaxedConsistency(boolean relaxedConsistency) {
        this.relaxedConsistency = relaxedConsistency;

        return this;
    }

    /**
     * Get whether to update file length on flush.
     * <p>
     * Controls whether to update file length or not when {@link IgfsOutputStream#flush()} method is invoked. You
     * may want to set this property to true in case you want to read from a file which is being written at the
     * same time.
     * <p>
     * Defaults to {@link #DFLT_UPDATE_FILE_LEN_ON_FLUSH}.
     *
     * @return Whether to update file length on flush.
     */
    public boolean isUpdateFileLengthOnFlush() {
        return updateFileLenOnFlush;
    }

    /**
     * Set whether to update file length on flush.
     * <p>
     * Set {@link #isUpdateFileLengthOnFlush()} for more information.
     *
     * @param updateFileLenOnFlush Whether to update file length on flush.
     * @return {@code this} for chaining.
     */
    public FileSystemConfiguration setUpdateFileLengthOnFlush(boolean updateFileLenOnFlush) {
        this.updateFileLenOnFlush = updateFileLenOnFlush;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FileSystemConfiguration.class, this);
    }
}
