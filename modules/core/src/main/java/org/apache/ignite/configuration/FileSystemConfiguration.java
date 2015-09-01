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

import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * {@code IGFS} configuration. More than one file system can be configured within grid.
 * {@code IGFS} configuration is provided via {@link IgniteConfiguration#getFileSystemConfiguration()}
 * method.
 */
public class FileSystemConfiguration {
    /** Default file system user name. */
    public static final String DFLT_USER_NAME = System.getProperty("user.name", "anonymous");

    /** Default fragmentizer throttling block length. */
    public static final long DFLT_FRAGMENTIZER_THROTTLING_BLOCK_LENGTH = 16 * 1024 * 1024;

    /** Default fragmentizer throttling delay. */
    public static final long DFLT_FRAGMENTIZER_THROTTLING_DELAY = 200;

    /** Default fragmentizer concurrent files. */
    public static final int DFLT_FRAGMENTIZER_CONCURRENT_FILES = 0;

    /** Default fragmentizer local writes ratio. */
    public static final float DFLT_FRAGMENTIZER_LOCAL_WRITES_RATIO = 0.8f;

    /** Fragmentizer enabled property. */
    public static final boolean DFLT_FRAGMENTIZER_ENABLED = true;

    /** Default batch size for logging. */
    public static final int DFLT_IGFS_LOG_BATCH_SIZE = 100;

    /** Default {@code IGFS} log directory. */
    public static final String DFLT_IGFS_LOG_DIR = "work/igfs/log";

    /** Default per node buffer size. */
    public static final int DFLT_PER_NODE_BATCH_SIZE = 100;

    /** Default number of per node parallel operations. */
    public static final int DFLT_PER_NODE_PARALLEL_BATCH_CNT = 8;

    /** Default IGFS mode. */
    public static final IgfsMode DFLT_MODE = IgfsMode.DUAL_ASYNC;

    /** Default file's data block size (bytes). */
    public static final int DFLT_BLOCK_SIZE = 1 << 16;

    /** Default read/write buffers size (bytes). */
    public static final int DFLT_BUF_SIZE = 1 << 16;

    /** Default trash directory purge await timeout in case data cache oversize is detected. */
    public static final long DFLT_TRASH_PURGE_TIMEOUT = 1000;

    /** Default management port. */
    public static final int DFLT_MGMT_PORT = 11400;

    /** Default IPC endpoint enabled flag. */
    public static final boolean DFLT_IPC_ENDPOINT_ENABLED = true;

    /** IGFS instance name. */
    private String name;

    /** Cache name to store IGFS meta information. */
    private String metaCacheName;

    /** Cache name to store file's data blocks. */
    private String dataCacheName;

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

    /** Fragmentizer local writes ratio. */
    private float fragmentizerLocWritesRatio = DFLT_FRAGMENTIZER_LOCAL_WRITES_RATIO;

    /** Fragmentizer enabled flag. */
    private boolean fragmentizerEnabled = DFLT_FRAGMENTIZER_ENABLED;

    /** Path modes. */
    private Map<String, IgfsMode> pathModes;

    /** Maximum space. */
    private long maxSpace;

    /** Trash purge await timeout. */
    private long trashPurgeTimeout = DFLT_TRASH_PURGE_TIMEOUT;

    /** Dual mode PUT operations executor service. */
    private ExecutorService dualModePutExec;

    /** Dual mode PUT operations executor service shutdown flag. */
    private boolean dualModePutExecShutdown;

    /** Maximum amount of data in pending puts. */
    private long dualModeMaxPendingPutsSize;

    /** Maximum range length. */
    private long maxTaskRangeLen;

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
        bufSize = cfg.getStreamBufferSize();
        dataCacheName = cfg.getDataCacheName();
        dfltMode = cfg.getDefaultMode();
        dualModeMaxPendingPutsSize = cfg.getDualModeMaxPendingPutsSize();
        dualModePutExec = cfg.getDualModePutExecutorService();
        dualModePutExecShutdown = cfg.getDualModePutExecutorServiceShutdown();
        fragmentizerConcurrentFiles = cfg.getFragmentizerConcurrentFiles();
        fragmentizerLocWritesRatio = cfg.getFragmentizerLocalWritesRatio();
        fragmentizerEnabled = cfg.isFragmentizerEnabled();
        fragmentizerThrottlingBlockLen = cfg.getFragmentizerThrottlingBlockLength();
        fragmentizerThrottlingDelay = cfg.getFragmentizerThrottlingDelay();
        secondaryFs = cfg.getSecondaryFileSystem();
        ipcEndpointCfg = cfg.getIpcEndpointConfiguration();
        ipcEndpointEnabled = cfg.isIpcEndpointEnabled();
        maxSpace = cfg.getMaxSpaceSize();
        maxTaskRangeLen = cfg.getMaximumTaskRangeLength();
        metaCacheName = cfg.getMetaCacheName();
        mgmtPort = cfg.getManagementPort();
        name = cfg.getName();
        pathModes = cfg.getPathModes();
        perNodeBatchSize = cfg.getPerNodeBatchSize();
        perNodeParallelBatchCnt = cfg.getPerNodeParallelBatchCount();
        prefetchBlocks = cfg.getPrefetchBlocks();
        seqReadsBeforePrefetch = cfg.getSequentialReadsBeforePrefetch();
        trashPurgeTimeout = cfg.getTrashPurgeTimeout();
    }

    /**
     * Gets IGFS instance name. If {@code null}, then instance with default
     * name will be used.
     *
     * @return IGFS instance name.
     */
    @Nullable public String getName() {
        return name;
    }

    /**
     * Sets IGFS instance name.
     *
     * @param name IGFS instance name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Cache name to store IGFS meta information. If {@code null}, then instance
     * with default meta-cache name will be used.
     *
     * @return Cache name to store IGFS meta information.
     */
    @Nullable public String getMetaCacheName() {
        return metaCacheName;
    }

    /**
     * Sets cache name to store IGFS meta information.
     *
     * @param metaCacheName Cache name to store IGFS meta information.
     */
    public void setMetaCacheName(String metaCacheName) {
        this.metaCacheName = metaCacheName;
    }

    /**
     * Cache name to store IGFS data.
     *
     * @return Cache name to store IGFS data.
     */
    @Nullable public String getDataCacheName() {
        return dataCacheName;
    }

    /**
     * Sets cache name to store IGFS data.
     *
     * @param dataCacheName Cache name to store IGFS data.
     */
    public void setDataCacheName(String dataCacheName) {
        this.dataCacheName = dataCacheName;
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
     */
    public void setBlockSize(int blockSize) {
        A.ensure(blockSize >= 0, "blockSize >= 0");

        this.blockSize = blockSize == 0 ? DFLT_BLOCK_SIZE : blockSize;
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
     */
    public void setPrefetchBlocks(int prefetchBlocks) {
        A.ensure(prefetchBlocks >= 0, "prefetchBlocks >= 0");

        this.prefetchBlocks = prefetchBlocks;
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
     */
    public void setSequentialReadsBeforePrefetch(int seqReadsBeforePrefetch) {
        A.ensure(seqReadsBeforePrefetch >= 0, "seqReadsBeforePrefetch >= 0");

        this.seqReadsBeforePrefetch = seqReadsBeforePrefetch;
    }

    /**
     * Get read/write buffer size for {@code IGFS} stream operations in bytes.
     *
     * @return Read/write buffers size (bytes).
     */
    public int getStreamBufferSize() {
        return bufSize;
    }

    /**
     * Sets read/write buffers size for {@code IGFS} stream operations (bytes).
     *
     * @param bufSize Read/write buffers size for stream operations (bytes) or {@code 0} to reset default value.
     */
    public void setStreamBufferSize(int bufSize) {
        A.ensure(bufSize >= 0, "bufSize >= 0");

        this.bufSize = bufSize == 0 ? DFLT_BUF_SIZE : bufSize;
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
     */
    public void setPerNodeBatchSize(int perNodeBatchSize) {
        this.perNodeBatchSize = perNodeBatchSize;
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
     */
    public void setPerNodeParallelBatchCount(int perNodeParallelBatchCnt) {
        this.perNodeParallelBatchCnt = perNodeParallelBatchCnt;
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
     */
    public void setIpcEndpointConfiguration(@Nullable IgfsIpcEndpointConfiguration ipcEndpointCfg) {
        this.ipcEndpointCfg = ipcEndpointCfg;
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
     */
    public void setIpcEndpointEnabled(boolean ipcEndpointEnabled) {
        this.ipcEndpointEnabled = ipcEndpointEnabled;
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
     */
    public void setManagementPort(int mgmtPort) {
        this.mgmtPort = mgmtPort;
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
     */
    public void setDefaultMode(IgfsMode dfltMode) {
        this.dfltMode = dfltMode;
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
     * @param fileSystem
     */
    public void setSecondaryFileSystem(IgfsSecondaryFileSystem fileSystem) {
        secondaryFs = fileSystem;
    }

    /**
     * Gets map of path prefixes to {@code IGFS} modes used for them.
     * <p>
     * If path doesn't correspond to any specified prefix or mappings are not provided, then
     * {@link #getDefaultMode()} is used.
     * <p>
     * Several folders under {@code '/apache/ignite'} folder have predefined mappings which cannot be overridden.
     * <li>{@code /apache/ignite/primary} and all it's sub-folders will always work in {@code PRIMARY} mode.</li>
     * <p>
     * And in case secondary file system URI is provided:
     * <li>{@code /apache/ignite/proxy} and all it's sub-folders will always work in {@code PROXY} mode.</li>
     * <li>{@code /apache/ignite/sync} and all it's sub-folders will always work in {@code DUAL_SYNC} mode.</li>
     * <li>{@code /apache/ignite/async} and all it's sub-folders will always work in {@code DUAL_ASYNC} mode.</li>
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
     */
    public void setPathModes(Map<String, IgfsMode> pathModes) {
        this.pathModes = pathModes;
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
     */
    public void setFragmentizerThrottlingBlockLength(long fragmentizerThrottlingBlockLen) {
        this.fragmentizerThrottlingBlockLen = fragmentizerThrottlingBlockLen;
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
     */
    public void setFragmentizerThrottlingDelay(long fragmentizerThrottlingDelay) {
        this.fragmentizerThrottlingDelay = fragmentizerThrottlingDelay;
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
     */
    public void setFragmentizerConcurrentFiles(int fragmentizerConcurrentFiles) {
        this.fragmentizerConcurrentFiles = fragmentizerConcurrentFiles;
    }

    /**
     * Gets amount of local memory (in % of local IGFS max space size) available for local writes
     * during file creation.
     * <p>
     * If current IGFS space size is less than {@code fragmentizerLocalWritesRatio * maxSpaceSize},
     * then file blocks will be written to the local node first and then asynchronously distributed
     * among cluster nodes (fragmentized).
     * <p>
     * Default value is {@link #DFLT_FRAGMENTIZER_LOCAL_WRITES_RATIO}.
     *
     * @return Ratio for local writes space.
     *
     * @deprecated Parameter is no longer used.
     */
    @Deprecated
    public float getFragmentizerLocalWritesRatio() {
        return fragmentizerLocWritesRatio;
    }

    /**
     * Sets ratio for space available for local file writes.
     *
     * @param fragmentizerLocWritesRatio Ratio for local file writes.
     * @see #getFragmentizerLocalWritesRatio()
     *
     * @deprecated Parameter is no longer used.
     */
    @Deprecated
    public void setFragmentizerLocalWritesRatio(float fragmentizerLocWritesRatio) {
        this.fragmentizerLocWritesRatio = fragmentizerLocWritesRatio;
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
     */
    public void setFragmentizerEnabled(boolean fragmentizerEnabled) {
        this.fragmentizerEnabled = fragmentizerEnabled;
    }

    /**
     * Get maximum space available for data cache to store file system entries.
     *
     * @return Maximum space available for data cache.
     */
    public long getMaxSpaceSize() {
        return maxSpace;
    }

    /**
     * Set maximum space in bytes available in data cache.
     *
     * @param maxSpace Maximum space available in data cache.
     */
    public void setMaxSpaceSize(long maxSpace) {
        this.maxSpace = maxSpace;
    }

    /**
     * Gets maximum timeout awaiting for trash purging in case data cache oversize is detected.
     *
     * @return Maximum timeout awaiting for trash purging in case data cache oversize is detected.
     */
    public long getTrashPurgeTimeout() {
        return trashPurgeTimeout;
    }

    /**
     * Sets maximum timeout awaiting for trash purging in case data cache oversize is detected.
     *
     * @param trashPurgeTimeout Maximum timeout awaiting for trash purging in case data cache oversize is detected.
     */
    public void setTrashPurgeTimeout(long trashPurgeTimeout) {
        this.trashPurgeTimeout = trashPurgeTimeout;
    }

    /**
     * Get DUAL mode put operation executor service. This executor service will process cache PUT requests for
     * data which came from the secondary file system and about to be written to IGFS data cache.
     * In case no executor service is provided, default one will be created with maximum amount of threads equals
     * to amount of processor cores.
     *
     * @return Get DUAL mode put operation executor service
     */
    @Nullable public ExecutorService getDualModePutExecutorService() {
        return dualModePutExec;
    }

    /**
     * Set DUAL mode put operations executor service.
     *
     * @param dualModePutExec Dual mode put operations executor service.
     */
    public void setDualModePutExecutorService(ExecutorService dualModePutExec) {
        this.dualModePutExec = dualModePutExec;
    }

    /**
     * Get DUAL mode put operation executor service shutdown flag.
     *
     * @return DUAL mode put operation executor service shutdown flag.
     */
    public boolean getDualModePutExecutorServiceShutdown() {
        return dualModePutExecShutdown;
    }

    /**
     * Set DUAL mode put operations executor service shutdown flag.
     *
     * @param dualModePutExecShutdown Dual mode put operations executor service shutdown flag.
     */
    public void setDualModePutExecutorServiceShutdown(boolean dualModePutExecShutdown) {
        this.dualModePutExecShutdown = dualModePutExecShutdown;
    }

    /**
     * Get maximum amount of pending data read from the secondary file system and waiting to be written to data
     * cache. {@code 0} or negative value stands for unlimited size.
     * <p>
     * By default this value is set to {@code 0}. It is recommended to set positive value in case your
     * application performs frequent reads of large amount of data from the secondary file system in order to
     * avoid issues with increasing GC pauses or out-of-memory error.
     *
     * @return Maximum amount of pending data read from the secondary file system
     */
    public long getDualModeMaxPendingPutsSize() {
        return dualModeMaxPendingPutsSize;
    }

    /**
     * Set maximum amount of data in pending put operations.
     *
     * @param dualModeMaxPendingPutsSize Maximum amount of data in pending put operations.
     */
    public void setDualModeMaxPendingPutsSize(long dualModeMaxPendingPutsSize) {
        this.dualModeMaxPendingPutsSize = dualModeMaxPendingPutsSize;
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
     */
    public void setMaximumTaskRangeLength(long maxTaskRangeLen) {
        this.maxTaskRangeLen = maxTaskRangeLen;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FileSystemConfiguration.class, this);
    }
}