/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.node;

import org.gridgain.grid.ggfs.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Data transfer object for GGFS configuration properties.
 */
public class VisorGgfsConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** GGFS instance name. */
    @Nullable private final String name;

    /** Cache name to store GGFS meta information. */
    @Nullable private final String metaCacheName;

    /** Cache name to store GGFS data. */
    @Nullable private final String dataCacheName;

    /** File's data block size. */
    private final int blockSize;

    /** Number of pre-fetched blocks if specific file's chunk is requested. */
    private final int prefetchBlocks;

    /** Read/write buffer size for GGFS stream operations in bytes. */
    private final int streamBufferSize;

    /** Number of file blocks buffered on local node before sending batch to remote node. */
    private final int perNodeBatchSize;

    /** Number of batches that can be concurrently sent to remote node. */
    private final int perNodeParallelBatchCount;

    /** URI of the secondary Hadoop file system. */
    @Nullable private final String secondaryHadoopFileSystemUri;

    /** Path for the secondary hadoop file system config. */
    @Nullable private final String secondaryHadoopFileSystemConfigPath;

    /** GGFS instance mode. */
    private final GridGgfsMode defaultMode;

    /** Map of paths to GGFS modes. */
    @Nullable private final Map<String, GridGgfsMode> pathModes;

    /** Dual mode PUT operations executor service. */
    private final String dualModePutExecutorService;

    /** Dual mode PUT operations executor service shutdown flag. */
    private final boolean dualModePutExecutorServiceShutdown;

    /** Maximum amount of data in pending puts. */
    private final long dualModeMaxPendingPutsSize;

    /** Maximum range length. */
    private final long maxTaskRangeLength;

    /** Fragmentizer concurrent files. */
    private final int fragmentizerConcurrentFiles;

    /** Fragmentizer local writes ratio. */
    private final float fragmentizerLocWritesRatio;

    /** Fragmentizer enabled flag. */
    private final boolean fragmentizerEnabled;

    /** Fragmentizer throttling block length. */
    private final long fragmentizerThrottlingBlockLen;

    /** Fragmentizer throttling delay. */
    private final long fragmentizerThrottlingDelay;

    /** IPC endpoint config (in JSON format) to publish GGFS over. */
    @Nullable private final String ipcEndpointCfg;

    /** IPC endpoint enabled flag. */
    private final boolean ipcEndpointEnabled;

    /** Maximum space. */
    private final long maxSpace;

    /** Management port. */
    private final int mgmtPort;

    /** Amount of sequential block reads before prefetch is triggered. */
    private final int seqReadsBeforePrefetch;

    /** Trash purge await timeout. */
    private final long trashPurgeTimeout;

    /** Create data transfer object with given parameters. */
    public VisorGgfsConfig(
        @Nullable String name,
        @Nullable String metaCacheName,
        @Nullable String dataCacheName,
        int blockSize,
        int prefetchBlocks,
        int streamBufferSize,
        int perNodeBatchSize,
        int perNodeParallelBatchCount,
        @Nullable String secondaryHadoopFileSystemUri,
        @Nullable String secondaryHadoopFileSystemConfigPath,
        GridGgfsMode defaultMode,
        @Nullable Map<String, GridGgfsMode> pathModes,
        String dualModePutExecutorService,
        boolean dualModePutExecutorServiceShutdown,
        long dualModeMaxPendingPutsSize,
        long maxTaskRangeLength,
        int fragmentizerConcurrentFiles,
        float fragmentizerLocWritesRatio,
        boolean fragmentizerEnabled,
        long fragmentizerThrottlingBlockLen,
        long fragmentizerThrottlingDelay,
        @Nullable String ipcEndpointCfg,
        boolean ipcEndpointEnabled,
        long maxSpace, int mgmtPort,
        int seqReadsBeforePrefetch,
        long trashPurgeTimeout
    ) {
        this.name = name;
        this.metaCacheName = metaCacheName;
        this.dataCacheName = dataCacheName;
        this.blockSize = blockSize;
        this.prefetchBlocks = prefetchBlocks;
        this.streamBufferSize = streamBufferSize;
        this.perNodeBatchSize = perNodeBatchSize;
        this.perNodeParallelBatchCount = perNodeParallelBatchCount;
        this.secondaryHadoopFileSystemUri = secondaryHadoopFileSystemUri;
        this.secondaryHadoopFileSystemConfigPath = secondaryHadoopFileSystemConfigPath;
        this.defaultMode = defaultMode;
        this.pathModes = pathModes;
        this.dualModePutExecutorService = dualModePutExecutorService;
        this.dualModePutExecutorServiceShutdown = dualModePutExecutorServiceShutdown;
        this.dualModeMaxPendingPutsSize = dualModeMaxPendingPutsSize;
        this.maxTaskRangeLength = maxTaskRangeLength;
        this.fragmentizerConcurrentFiles = fragmentizerConcurrentFiles;
        this.fragmentizerLocWritesRatio = fragmentizerLocWritesRatio;
        this.fragmentizerEnabled = fragmentizerEnabled;
        this.fragmentizerThrottlingBlockLen = fragmentizerThrottlingBlockLen;
        this.fragmentizerThrottlingDelay = fragmentizerThrottlingDelay;
        this.ipcEndpointCfg = ipcEndpointCfg;
        this.ipcEndpointEnabled = ipcEndpointEnabled;
        this.maxSpace = maxSpace;
        this.mgmtPort = mgmtPort;
        this.seqReadsBeforePrefetch = seqReadsBeforePrefetch;
        this.trashPurgeTimeout = trashPurgeTimeout;
    }

    /**
     * @return GGFS instance name.
     */
    public String name() {
        return name;
    }

    /**
     * @return Cache name to store GGFS meta information.
     */
    public String metaCacheName() {
        return metaCacheName;
    }

    /**
     * @return Cache name to store GGFS data.
     */
    public String dataCacheName() {
        return dataCacheName;
    }

    /**
     * @return File's data block size.
     */
    public int blockSize() {
        return blockSize;
    }

    /**
     * @return Number of pre-fetched blocks if specific file's chunk is requested.
     */
    public int prefetchBlocks() {
        return prefetchBlocks;
    }

    /**
     * @return Read/write buffer size for GGFS stream operations in bytes.
     */
    public int streamBufferSize() {
        return streamBufferSize;
    }

    /**
     * @return Number of file blocks buffered on local node before sending batch to remote node.
     */
    public int perNodeBatchSize() {
        return perNodeBatchSize;
    }

    /**
     * @return Number of batches that can be concurrently sent to remote node.
     */
    public int perNodeParallelBatchCount() {
        return perNodeParallelBatchCount;
    }

    /**
     * @return URI of the secondary Hadoop file system.
     */
    public String secondaryHadoopFileSystemUri() {
        return secondaryHadoopFileSystemUri;
    }

    /**
     * @return Path for the secondary hadoop file system config.
     */
    public String secondaryHadoopFileSystemConfigPath() {
        return secondaryHadoopFileSystemConfigPath;
    }

    /**
     * @return GGFS instance mode.
     */
    public GridGgfsMode defaultMode() {
        return defaultMode;
    }

    /**
     * @return Map of paths to GGFS modes.
     */
    @Nullable public Map<String, GridGgfsMode> pathModes() {
        return pathModes;
    }

    /**
     * @return Dual mode PUT operations executor service.
     */
    public String dualModePutExecutorService() {
        return dualModePutExecutorService;
    }

    /**
     * @return Dual mode PUT operations executor service shutdown flag.
     */
    public boolean dualModePutExecutorServiceShutdown() {
        return dualModePutExecutorServiceShutdown;
    }

    /**
     * @return Maximum amount of data in pending puts.
     */
    public long dualModeMaxPendingPutsSize() {
        return dualModeMaxPendingPutsSize;
    }

    /**
     * @return Maximum range length.
     */
    public long maxTaskRangeLength() {
        return maxTaskRangeLength;
    }

    /**
     * @return Fragmentizer concurrent files.
     */
    public int fragmentizerConcurrentFiles() {
        return fragmentizerConcurrentFiles;
    }

    /**
     * @return Fragmentizer local writes ratio.
     */
    public float fragmentizerLocalWritesRatio() {
        return fragmentizerLocWritesRatio;
    }

    /**
     * @return Fragmentizer enabled flag.
     */
    public boolean fragmentizerEnabled() {
        return fragmentizerEnabled;
    }

    /**
     * @return Fragmentizer throttling block length.
     */
    public long fragmentizerThrottlingBlockLength() {
        return fragmentizerThrottlingBlockLen;
    }

    /**
     * @return Fragmentizer throttling delay.
     */
    public long fragmentizerThrottlingDelay() {
        return fragmentizerThrottlingDelay;
    }

    /**
     * @return IPC endpoint config (in JSON format) to publish GGFS over.
     */
    public String ipcEndpointConfig() {
        return ipcEndpointCfg;
    }

    /**
     * @return IPC endpoint enabled flag.
     */
    public boolean ipcEndpointEnabled() {
        return ipcEndpointEnabled;
    }

    /**
     * @return Maximum space.
     */
    public long maxSpace() {
        return maxSpace;
    }

    /**
     * @return Management port.
     */
    public int mgmtPort() {
        return mgmtPort;
    }

    /**
     * @return Amount of sequential block reads before prefetch is triggered.
     */
    public int sequenceReadsBeforePrefetch() {
        return seqReadsBeforePrefetch;
    }

    /**
     * @return Trash purge await timeout.
     */
    public long trashPurgeTimeout() {
        return trashPurgeTimeout;
    }
}
