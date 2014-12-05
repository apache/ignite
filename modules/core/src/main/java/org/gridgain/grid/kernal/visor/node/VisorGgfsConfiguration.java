/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.node;

import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.kernal.visor.util.VisorTaskUtils.*;

/**
 * Data transfer object for GGFS configuration properties.
 */
public class VisorGgfsConfiguration implements Serializable {
    /** Property name for path to Hadoop configuration. */
    public static final String SECONDARY_FS_CONFIG_PATH = "SECONDARY_FS_CONFIG_PATH";

    /** Property name for URI of file system. */
    public static final String SECONDARY_FS_URI = "SECONDARY_FS_URI";

    /** */
    private static final long serialVersionUID = 0L;

    /** GGFS instance name. */
    private String name;

    /** Cache name to store GGFS meta information. */
    private String metaCacheName;

    /** Cache name to store GGFS data. */
    private String dataCacheName;

    /** File's data block size. */
    private int blockSize;

    /** Number of pre-fetched blocks if specific file's chunk is requested. */
    private int prefetchBlocks;

    /** Read/write buffer size for GGFS stream operations in bytes. */
    private int streamBufferSize;

    /** Number of file blocks buffered on local node before sending batch to remote node. */
    private int perNodeBatchSize;

    /** Number of batches that can be concurrently sent to remote node. */
    private int perNodeParallelBatchCount;

    /** URI of the secondary Hadoop file system. */
    private String secondaryHadoopFileSystemUri;

    /** Path for the secondary hadoop file system config. */
    private String secondaryHadoopFileSystemConfigPath;

    /** GGFS instance mode. */
    private GridGgfsMode defaultMode;

    /** Map of paths to GGFS modes. */
    private Map<String, GridGgfsMode> pathModes;

    /** Dual mode PUT operations executor service. */
    private String dualModePutExecutorService;

    /** Dual mode PUT operations executor service shutdown flag. */
    private boolean dualModePutExecutorServiceShutdown;

    /** Maximum amount of data in pending puts. */
    private long dualModeMaxPendingPutsSize;

    /** Maximum range length. */
    private long maxTaskRangeLength;

    /** Fragmentizer concurrent files. */
    private int fragmentizerConcurrentFiles;

    /** Fragmentizer local writes ratio. */
    private float fragmentizerLocWritesRatio;

    /** Fragmentizer enabled flag. */
    private boolean fragmentizerEnabled;

    /** Fragmentizer throttling block length. */
    private long fragmentizerThrottlingBlockLen;

    /** Fragmentizer throttling delay. */
    private long fragmentizerThrottlingDelay;

    /** IPC endpoint config (in JSON format) to publish GGFS over. */
    private String ipcEndpointCfg;

    /** IPC endpoint enabled flag. */
    private boolean ipcEndpointEnabled;

    /** Maximum space. */
    private long maxSpace;

    /** Management port. */
    private int mgmtPort;

    /** Amount of sequential block reads before prefetch is triggered. */
    private int seqReadsBeforePrefetch;

    /** Trash purge await timeout. */
    private long trashPurgeTimeout;

    /**
     * @param ggfs GGFS configuration.
     * @return Data transfer object for GGFS configuration properties.
     */
    public static VisorGgfsConfiguration from(IgniteFsConfiguration ggfs) {
        VisorGgfsConfiguration cfg = new VisorGgfsConfiguration();

        cfg.name(ggfs.getName());
        cfg.metaCacheName(ggfs.getMetaCacheName());
        cfg.dataCacheName(ggfs.getDataCacheName());
        cfg.blockSize(ggfs.getBlockSize());
        cfg.prefetchBlocks(ggfs.getPrefetchBlocks());
        cfg.streamBufferSize(ggfs.getStreamBufferSize());
        cfg.perNodeBatchSize(ggfs.getPerNodeBatchSize());
        cfg.perNodeParallelBatchCount(ggfs.getPerNodeParallelBatchCount());

        GridGgfsFileSystem secFs = ggfs.getSecondaryFileSystem();

        if (secFs != null) {
            Map<String, String> props = secFs.properties();

            cfg.secondaryHadoopFileSystemUri(props.get(SECONDARY_FS_URI));
            cfg.secondaryHadoopFileSystemConfigPath(props.get(SECONDARY_FS_CONFIG_PATH));
        }

        cfg.defaultMode(ggfs.getDefaultMode());
        cfg.pathModes(ggfs.getPathModes());
        cfg.dualModePutExecutorService(compactClass(ggfs.getDualModePutExecutorService()));
        cfg.dualModePutExecutorServiceShutdown(ggfs.getDualModePutExecutorServiceShutdown());
        cfg.dualModeMaxPendingPutsSize(ggfs.getDualModeMaxPendingPutsSize());
        cfg.maxTaskRangeLength(ggfs.getMaximumTaskRangeLength());
        cfg.fragmentizerConcurrentFiles(ggfs.getFragmentizerConcurrentFiles());
        cfg.fragmentizerLocalWritesRatio(ggfs.getFragmentizerLocalWritesRatio());
        cfg.fragmentizerEnabled(ggfs.isFragmentizerEnabled());
        cfg.fragmentizerThrottlingBlockLength(ggfs.getFragmentizerThrottlingBlockLength());
        cfg.fragmentizerThrottlingDelay(ggfs.getFragmentizerThrottlingDelay());

        Map<String, String> endpointCfg = ggfs.getIpcEndpointConfiguration();
        cfg.ipcEndpointConfiguration(endpointCfg != null ? endpointCfg.toString() : null);

        cfg.ipcEndpointEnabled(ggfs.isIpcEndpointEnabled());
        cfg.maxSpace(ggfs.getMaxSpaceSize());
        cfg.managementPort(ggfs.getManagementPort());
        cfg.sequenceReadsBeforePrefetch(ggfs.getSequentialReadsBeforePrefetch());
        cfg.trashPurgeTimeout(ggfs.getTrashPurgeTimeout());

        return cfg;
    }

    /**
     * Construct data transfer object for ggfs configurations properties.
     *
     * @param ggfss ggfs configurations.
     * @return ggfs configurations properties.
     */
    public static Iterable<VisorGgfsConfiguration> list(IgniteFsConfiguration[] ggfss) {
        if (ggfss == null)
            return Collections.emptyList();

        final Collection<VisorGgfsConfiguration> cfgs = new ArrayList<>(ggfss.length);

        for (IgniteFsConfiguration ggfs : ggfss)
            cfgs.add(from(ggfs));

        return cfgs;
    }

    /**
     * @return GGFS instance name.
     */
    @Nullable public String name() {
        return name;
    }

    /**
     * @param name New gGFS instance name.
     */
    public void name(@Nullable String name) {
        this.name = name;
    }

    /**
     * @return Cache name to store GGFS meta information.
     */
    @Nullable public String metaCacheName() {
        return metaCacheName;
    }

    /**
     * @param metaCacheName New cache name to store GGFS meta information.
     */
    public void metaCacheName(@Nullable String metaCacheName) {
        this.metaCacheName = metaCacheName;
    }

    /**
     * @return Cache name to store GGFS data.
     */
    @Nullable public String dataCacheName() {
        return dataCacheName;
    }

    /**
     * @param dataCacheName New cache name to store GGFS data.
     */
    public void dataCacheName(@Nullable String dataCacheName) {
        this.dataCacheName = dataCacheName;
    }

    /**
     * @return File's data block size.
     */
    public int blockSize() {
        return blockSize;
    }

    /**
     * @param blockSize New file's data block size.
     */
    public void blockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    /**
     * @return Number of pre-fetched blocks if specific file's chunk is requested.
     */
    public int prefetchBlocks() {
        return prefetchBlocks;
    }

    /**
     * @param prefetchBlocks New number of pre-fetched blocks if specific file's chunk is requested.
     */
    public void prefetchBlocks(int prefetchBlocks) {
        this.prefetchBlocks = prefetchBlocks;
    }

    /**
     * @return Read/write buffer size for GGFS stream operations in bytes.
     */
    public int streamBufferSize() {
        return streamBufferSize;
    }

    /**
     * @param streamBufSize New read/write buffer size for GGFS stream operations in bytes.
     */
    public void streamBufferSize(int streamBufSize) {
        streamBufferSize = streamBufSize;
    }

    /**
     * @return Number of file blocks buffered on local node before sending batch to remote node.
     */
    public int perNodeBatchSize() {
        return perNodeBatchSize;
    }

    /**
     * @param perNodeBatchSize New number of file blocks buffered on local node before sending batch to remote node.
     */
    public void perNodeBatchSize(int perNodeBatchSize) {
        this.perNodeBatchSize = perNodeBatchSize;
    }

    /**
     * @return Number of batches that can be concurrently sent to remote node.
     */
    public int perNodeParallelBatchCount() {
        return perNodeParallelBatchCount;
    }

    /**
     * @param perNodeParallelBatchCnt New number of batches that can be concurrently sent to remote node.
     */
    public void perNodeParallelBatchCount(int perNodeParallelBatchCnt) {
        perNodeParallelBatchCount = perNodeParallelBatchCnt;
    }

    /**
     * @return URI of the secondary Hadoop file system.
     */
    @Nullable public String secondaryHadoopFileSystemUri() {
        return secondaryHadoopFileSystemUri;
    }

    /**
     * @param secondaryHadoopFileSysUri New URI of the secondary Hadoop file system.
     */
    public void secondaryHadoopFileSystemUri(@Nullable String secondaryHadoopFileSysUri) {
        secondaryHadoopFileSystemUri = secondaryHadoopFileSysUri;
    }

    /**
     * @return Path for the secondary hadoop file system config.
     */
    @Nullable public String secondaryHadoopFileSystemConfigPath() {
        return secondaryHadoopFileSystemConfigPath;
    }

    /**
     * @param secondaryHadoopFileSysCfgPath New path for the secondary hadoop file system config.
     */
    public void secondaryHadoopFileSystemConfigPath(@Nullable String secondaryHadoopFileSysCfgPath) {
        secondaryHadoopFileSystemConfigPath = secondaryHadoopFileSysCfgPath;
    }

    /**
     * @return GGFS instance mode.
     */
    public GridGgfsMode defaultMode() {
        return defaultMode;
    }

    /**
     * @param dfltMode New gGFS instance mode.
     */
    public void defaultMode(GridGgfsMode dfltMode) {
        defaultMode = dfltMode;
    }

    /**
     * @return Map of paths to GGFS modes.
     */
    @Nullable public Map<String, GridGgfsMode> pathModes() {
        return pathModes;
    }

    /**
     * @param pathModes New map of paths to GGFS modes.
     */
    public void pathModes(@Nullable Map<String, GridGgfsMode> pathModes) {
        this.pathModes = pathModes;
    }

    /**
     * @return Dual mode PUT operations executor service.
     */
    public String dualModePutExecutorService() {
        return dualModePutExecutorService;
    }

    /**
     * @param dualModePutExecutorSrvc New dual mode PUT operations executor service.
     */
    public void dualModePutExecutorService(String dualModePutExecutorSrvc) {
        dualModePutExecutorService = dualModePutExecutorSrvc;
    }

    /**
     * @return Dual mode PUT operations executor service shutdown flag.
     */
    public boolean dualModePutExecutorServiceShutdown() {
        return dualModePutExecutorServiceShutdown;
    }

    /**
     * @param dualModePutExecutorSrvcShutdown New dual mode PUT operations executor service shutdown flag.
     */
    public void dualModePutExecutorServiceShutdown(boolean dualModePutExecutorSrvcShutdown) {
        dualModePutExecutorServiceShutdown = dualModePutExecutorSrvcShutdown;
    }

    /**
     * @return Maximum amount of data in pending puts.
     */
    public long dualModeMaxPendingPutsSize() {
        return dualModeMaxPendingPutsSize;
    }

    /**
     * @param dualModeMaxPendingPutsSize New maximum amount of data in pending puts.
     */
    public void dualModeMaxPendingPutsSize(long dualModeMaxPendingPutsSize) {
        this.dualModeMaxPendingPutsSize = dualModeMaxPendingPutsSize;
    }

    /**
     * @return Maximum range length.
     */
    public long maxTaskRangeLength() {
        return maxTaskRangeLength;
    }

    /**
     * @param maxTaskRangeLen New maximum range length.
     */
    public void maxTaskRangeLength(long maxTaskRangeLen) {
        maxTaskRangeLength = maxTaskRangeLen;
    }

    /**
     * @return Fragmentizer concurrent files.
     */
    public int fragmentizerConcurrentFiles() {
        return fragmentizerConcurrentFiles;
    }

    /**
     * @param fragmentizerConcurrentFiles New fragmentizer concurrent files.
     */
    public void fragmentizerConcurrentFiles(int fragmentizerConcurrentFiles) {
        this.fragmentizerConcurrentFiles = fragmentizerConcurrentFiles;
    }

    /**
     * @return Fragmentizer local writes ratio.
     */
    public float fragmentizerLocalWritesRatio() {
        return fragmentizerLocWritesRatio;
    }

    /**
     * @param fragmentizerLocWritesRatio New fragmentizer local writes ratio.
     */
    public void fragmentizerLocalWritesRatio(float fragmentizerLocWritesRatio) {
        this.fragmentizerLocWritesRatio = fragmentizerLocWritesRatio;
    }

    /**
     * @return Fragmentizer enabled flag.
     */
    public boolean fragmentizerEnabled() {
        return fragmentizerEnabled;
    }

    /**
     * @param fragmentizerEnabled New fragmentizer enabled flag.
     */
    public void fragmentizerEnabled(boolean fragmentizerEnabled) {
        this.fragmentizerEnabled = fragmentizerEnabled;
    }

    /**
     * @return Fragmentizer throttling block length.
     */
    public long fragmentizerThrottlingBlockLength() {
        return fragmentizerThrottlingBlockLen;
    }

    /**
     * @param fragmentizerThrottlingBlockLen New fragmentizer throttling block length.
     */
    public void fragmentizerThrottlingBlockLength(long fragmentizerThrottlingBlockLen) {
        this.fragmentizerThrottlingBlockLen = fragmentizerThrottlingBlockLen;
    }

    /**
     * @return Fragmentizer throttling delay.
     */
    public long fragmentizerThrottlingDelay() {
        return fragmentizerThrottlingDelay;
    }

    /**
     * @param fragmentizerThrottlingDelay New fragmentizer throttling delay.
     */
    public void fragmentizerThrottlingDelay(long fragmentizerThrottlingDelay) {
        this.fragmentizerThrottlingDelay = fragmentizerThrottlingDelay;
    }

    /**
     * @return IPC endpoint config (in JSON format) to publish GGFS over.
     */
    @Nullable public String ipcEndpointConfiguration() {
        return ipcEndpointCfg;
    }

    /**
     * @param ipcEndpointCfg New IPC endpoint config (in JSON format) to publish GGFS over.
     */
    public void ipcEndpointConfiguration(@Nullable String ipcEndpointCfg) {
        this.ipcEndpointCfg = ipcEndpointCfg;
    }

    /**
     * @return IPC endpoint enabled flag.
     */
    public boolean ipcEndpointEnabled() {
        return ipcEndpointEnabled;
    }

    /**
     * @param ipcEndpointEnabled New iPC endpoint enabled flag.
     */
    public void ipcEndpointEnabled(boolean ipcEndpointEnabled) {
        this.ipcEndpointEnabled = ipcEndpointEnabled;
    }

    /**
     * @return Maximum space.
     */
    public long maxSpace() {
        return maxSpace;
    }

    /**
     * @param maxSpace New maximum space.
     */
    public void maxSpace(long maxSpace) {
        this.maxSpace = maxSpace;
    }

    /**
     * @return Management port.
     */
    public int managementPort() {
        return mgmtPort;
    }

    /**
     * @param mgmtPort New management port.
     */
    public void managementPort(int mgmtPort) {
        this.mgmtPort = mgmtPort;
    }

    /**
     * @return Amount of sequential block reads before prefetch is triggered.
     */
    public int sequenceReadsBeforePrefetch() {
        return seqReadsBeforePrefetch;
    }

    /**
     * @param seqReadsBeforePrefetch New amount of sequential block reads before prefetch is triggered.
     */
    public void sequenceReadsBeforePrefetch(int seqReadsBeforePrefetch) {
        this.seqReadsBeforePrefetch = seqReadsBeforePrefetch;
    }

    /**
     * @return Trash purge await timeout.
     */
    public long trashPurgeTimeout() {
        return trashPurgeTimeout;
    }

    /**
     * @param trashPurgeTimeout New trash purge await timeout.
     */
    public void trashPurgeTimeout(long trashPurgeTimeout) {
        this.trashPurgeTimeout = trashPurgeTimeout;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorGgfsConfiguration.class, this);
    }

}
