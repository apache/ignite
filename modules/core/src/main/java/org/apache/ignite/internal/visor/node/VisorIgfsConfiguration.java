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

package org.apache.ignite.internal.visor.node;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.internal.LessNamingBean;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;

/**
 * Data transfer object for IGFS configuration properties.
 */
public class VisorIgfsConfiguration implements Serializable, LessNamingBean {
    /** */
    private static final long serialVersionUID = 0L;

    /** IGFS instance name. */
    private String name;

    /** Cache name to store IGFS meta information. */
    private String metaCacheName;

    /** Cache name to store IGFS data. */
    private String dataCacheName;

    /** File's data block size. */
    private int blockSize;

    /** Number of pre-fetched blocks if specific file's chunk is requested. */
    private int prefetchBlocks;

    /** Read/write buffer size for IGFS stream operations in bytes. */
    private int streamBufSize;

    /** Number of file blocks buffered on local node before sending batch to remote node. */
    private int perNodeBatchSize;

    /** Number of batches that can be concurrently sent to remote node. */
    private int perNodeParallelBatchCnt;

    /** @deprecated Needed only for backward compatibility. */
    private String secondaryHadoopFileSysUri;

    /** @deprecated Needed only for backward compatibility. */
    private String secondaryHadoopFileSysCfgPath;

    /** @deprecated Needed only for backward compatibility. */
    private String secondaryHadoopFileSysUserName;

    /** IGFS instance mode. */
    private IgfsMode dfltMode;

    /** Map of paths to IGFS modes. */
    private Map<String, IgfsMode> pathModes;

    /** Dual mode PUT operations executor service. */
    private String dualModePutExecutorSrvc;

    /** Dual mode PUT operations executor service shutdown flag. */
    private boolean dualModePutExecutorSrvcShutdown;

    /** Maximum amount of data in pending puts. */
    private long dualModeMaxPendingPutsSize;

    /** Maximum range length. */
    private long maxTaskRangeLen;

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

    /** IPC endpoint config (in JSON format) to publish IGFS over. */
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
     * @param igfs IGFS configuration.
     * @return Data transfer object for IGFS configuration properties.
     */
    public static VisorIgfsConfiguration from(FileSystemConfiguration igfs) {
        VisorIgfsConfiguration cfg = new VisorIgfsConfiguration();

        cfg.name = igfs.getName();
        cfg.metaCacheName = igfs.getMetaCacheName();
        cfg.dataCacheName = igfs.getDataCacheName();
        cfg.blockSize = igfs.getBlockSize();
        cfg.prefetchBlocks = igfs.getPrefetchBlocks();
        cfg.streamBufSize = igfs.getStreamBufferSize();
        cfg.perNodeBatchSize = igfs.getPerNodeBatchSize();
        cfg.perNodeParallelBatchCnt = igfs.getPerNodeParallelBatchCount();

        cfg.dfltMode = igfs.getDefaultMode();
        cfg.pathModes = igfs.getPathModes();
        cfg.dualModePutExecutorSrvc = compactClass(igfs.getDualModePutExecutorService());
        cfg.dualModePutExecutorSrvcShutdown = igfs.getDualModePutExecutorServiceShutdown();
        cfg.dualModeMaxPendingPutsSize = igfs.getDualModeMaxPendingPutsSize();
        cfg.maxTaskRangeLen = igfs.getMaximumTaskRangeLength();
        cfg.fragmentizerConcurrentFiles = igfs.getFragmentizerConcurrentFiles();
        cfg.fragmentizerLocWritesRatio = igfs.getFragmentizerLocalWritesRatio();
        cfg.fragmentizerEnabled = igfs.isFragmentizerEnabled();
        cfg.fragmentizerThrottlingBlockLen = igfs.getFragmentizerThrottlingBlockLength();
        cfg.fragmentizerThrottlingDelay = igfs.getFragmentizerThrottlingDelay();

        IgfsIpcEndpointConfiguration endpointCfg = igfs.getIpcEndpointConfiguration();

        cfg.ipcEndpointCfg = endpointCfg != null ? endpointCfg.toString() : null;

        cfg.ipcEndpointEnabled = igfs.isIpcEndpointEnabled();
        cfg.maxSpace = igfs.getMaxSpaceSize();
        cfg.mgmtPort = igfs.getManagementPort();
        cfg.seqReadsBeforePrefetch = igfs.getSequentialReadsBeforePrefetch();
        cfg.trashPurgeTimeout = igfs.getTrashPurgeTimeout();

        return cfg;
    }

    /**
     * Construct data transfer object for igfs configurations properties.
     *
     * @param igfss Igfs configurations.
     * @return igfs configurations properties.
     */
    public static Iterable<VisorIgfsConfiguration> list(FileSystemConfiguration[] igfss) {
        if (igfss == null)
            return Collections.emptyList();

        final Collection<VisorIgfsConfiguration> cfgs = new ArrayList<>(igfss.length);

        for (FileSystemConfiguration igfs : igfss)
            cfgs.add(from(igfs));

        return cfgs;
    }

    /**
     * @return IGFS instance name.
     */
    @Nullable public String name() {
        return name;
    }

    /**
     * @return Cache name to store IGFS meta information.
     */
    @Nullable public String metaCacheName() {
        return metaCacheName;
    }

    /**
     * @return Cache name to store IGFS data.
     */
    @Nullable public String dataCacheName() {
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
     * @return Read/write buffer size for IGFS stream operations in bytes.
     */
    public int streamBufferSize() {
        return streamBufSize;
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
        return perNodeParallelBatchCnt;
    }

    /**
     * @deprecated Needed only for backward compatibility.
     */
    @Nullable public String secondaryHadoopFileSystemUri() {
        return secondaryHadoopFileSysUri;
    }

    /**
     * @deprecated Needed only for backward compatibility.
     */
    @Nullable public String secondaryHadoopFileSystemUserName() {
        return secondaryHadoopFileSysUserName;
    }

    /**
     * @deprecated Needed only for backward compatibility.
     */
    @Nullable public String secondaryHadoopFileSystemConfigPath() {
        return secondaryHadoopFileSysCfgPath;
    }

    /**
     * @return IGFS instance mode.
     */
    public IgfsMode defaultMode() {
        return dfltMode;
    }

    /**
     * @return Map of paths to IGFS modes.
     */
    @Nullable public Map<String, IgfsMode> pathModes() {
        return pathModes;
    }

    /**
     * @return Dual mode PUT operations executor service.
     */
    public String dualModePutExecutorService() {
        return dualModePutExecutorSrvc;
    }

    /**
     * @return Dual mode PUT operations executor service shutdown flag.
     */
    public boolean dualModePutExecutorServiceShutdown() {
        return dualModePutExecutorSrvcShutdown;
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
        return maxTaskRangeLen;
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
     * @return IPC endpoint config to publish IGFS over.
     */
    @Nullable public String ipcEndpointConfiguration() {
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
    public int managementPort() {
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorIgfsConfiguration.class, this);
    }
}
