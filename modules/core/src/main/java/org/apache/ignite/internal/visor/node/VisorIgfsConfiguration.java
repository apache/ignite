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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;

/**
 * Data transfer object for IGFS configuration properties.
 */
public class VisorIgfsConfiguration extends VisorDataTransferObject {
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

    /** IGFS instance mode. */
    private IgfsMode dfltMode;

    /** Map of paths to IGFS modes. */
    private Map<String, IgfsMode> pathModes;

    /** Maximum range length. */
    private long maxTaskRangeLen;

    /** Fragmentizer concurrent files. */
    private int fragmentizerConcurrentFiles;

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

    /** Management port. */
    private int mgmtPort;

    /** Amount of sequential block reads before prefetch is triggered. */
    private int seqReadsBeforePrefetch;

    /**
     * Default constructor.
     */
    public VisorIgfsConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for IGFS configuration properties.
     * @param igfs IGFS configuration.
     */
    public VisorIgfsConfiguration(FileSystemConfiguration igfs) {
        name = igfs.getName();
        metaCacheName = igfs.getMetaCacheConfiguration().getName();
        dataCacheName = igfs.getDataCacheConfiguration().getName();
        blockSize = igfs.getBlockSize();
        prefetchBlocks = igfs.getPrefetchBlocks();
        streamBufSize = igfs.getBufferSize();
        perNodeBatchSize = igfs.getPerNodeBatchSize();
        perNodeParallelBatchCnt = igfs.getPerNodeParallelBatchCount();

        dfltMode = igfs.getDefaultMode();
        pathModes = igfs.getPathModes();
        maxTaskRangeLen = igfs.getMaximumTaskRangeLength();
        fragmentizerConcurrentFiles = igfs.getFragmentizerConcurrentFiles();
        fragmentizerEnabled = igfs.isFragmentizerEnabled();
        fragmentizerThrottlingBlockLen = igfs.getFragmentizerThrottlingBlockLength();
        fragmentizerThrottlingDelay = igfs.getFragmentizerThrottlingDelay();

        IgfsIpcEndpointConfiguration endpointCfg = igfs.getIpcEndpointConfiguration();

        ipcEndpointCfg = endpointCfg != null ? endpointCfg.toString() : null;

        ipcEndpointEnabled = igfs.isIpcEndpointEnabled();
        mgmtPort = igfs.getManagementPort();
        seqReadsBeforePrefetch = igfs.getSequentialReadsBeforePrefetch();
    }

    /**
     * Construct data transfer object for igfs configurations properties.
     *
     * @param igfss Igfs configurations.
     * @return igfs configurations properties.
     */
    public static List<VisorIgfsConfiguration> list(FileSystemConfiguration[] igfss) {
        List<VisorIgfsConfiguration> res = new ArrayList<>();

        if (!F.isEmpty(igfss)) {
            for (FileSystemConfiguration igfs : igfss)
                res.add(new VisorIgfsConfiguration(igfs));
        }

        return res;
    }

    /**
     * @return IGFS instance name.
     */
    @Nullable public String getName() {
        return name;
    }

    /**
     * @return Cache name to store IGFS meta information.
     */
    @Nullable public String getMetaCacheName() {
        return metaCacheName;
    }

    /**
     * @return Cache name to store IGFS data.
     */
    @Nullable public String getDataCacheName() {
        return dataCacheName;
    }

    /**
     * @return File's data block size.
     */
    public int getBlockSize() {
        return blockSize;
    }

    /**
     * @return Number of pre-fetched blocks if specific file's chunk is requested.
     */
    public int getPrefetchBlocks() {
        return prefetchBlocks;
    }

    /**
     * @return Read/write buffer size for IGFS stream operations in bytes.
     */
    public int getStreamBufferSize() {
        return streamBufSize;
    }

    /**
     * @return Number of file blocks buffered on local node before sending batch to remote node.
     */
    public int getPerNodeBatchSize() {
        return perNodeBatchSize;
    }

    /**
     * @return Number of batches that can be concurrently sent to remote node.
     */
    public int getPerNodeParallelBatchCount() {
        return perNodeParallelBatchCnt;
    }

    /**
     * @return IGFS instance mode.
     */
    public IgfsMode getDefaultMode() {
        return dfltMode;
    }

    /**
     * @return Map of paths to IGFS modes.
     */
    @Nullable public Map<String, IgfsMode> getPathModes() {
        return pathModes;
    }

    /**
     * @return Maximum range length.
     */
    public long getMaxTaskRangeLength() {
        return maxTaskRangeLen;
    }

    /**
     * @return Fragmentizer concurrent files.
     */
    public int getFragmentizerConcurrentFiles() {
        return fragmentizerConcurrentFiles;
    }

    /**
     * @return Fragmentizer enabled flag.
     */
    public boolean isFragmentizerEnabled() {
        return fragmentizerEnabled;
    }

    /**
     * @return Fragmentizer throttling block length.
     */
    public long getFragmentizerThrottlingBlockLength() {
        return fragmentizerThrottlingBlockLen;
    }

    /**
     * @return Fragmentizer throttling delay.
     */
    public long getFragmentizerThrottlingDelay() {
        return fragmentizerThrottlingDelay;
    }

    /**
     * @return IPC endpoint config to publish IGFS over.
     */
    @Nullable public String getIpcEndpointConfiguration() {
        return ipcEndpointCfg;
    }

    /**
     * @return IPC endpoint enabled flag.
     */
    public boolean isIpcEndpointEnabled() {
        return ipcEndpointEnabled;
    }

    /**
     * @return Management port.
     */
    public int getManagementPort() {
        return mgmtPort;
    }

    /**
     * @return Amount of sequential block reads before prefetch is triggered.
     */
    public int getSequenceReadsBeforePrefetch() {
        return seqReadsBeforePrefetch;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, name);
        U.writeString(out, metaCacheName);
        U.writeString(out, dataCacheName);
        out.writeInt(blockSize);
        out.writeInt(prefetchBlocks);
        out.writeInt(streamBufSize);
        out.writeInt(perNodeBatchSize);
        out.writeInt(perNodeParallelBatchCnt);
        U.writeEnum(out, dfltMode);
        U.writeMap(out, pathModes);
        out.writeLong(maxTaskRangeLen);
        out.writeInt(fragmentizerConcurrentFiles);
        out.writeBoolean(fragmentizerEnabled);
        out.writeLong(fragmentizerThrottlingBlockLen);
        out.writeLong(fragmentizerThrottlingDelay);
        U.writeString(out, ipcEndpointCfg);
        out.writeBoolean(ipcEndpointEnabled);
        out.writeInt(mgmtPort);
        out.writeInt(seqReadsBeforePrefetch);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        name = U.readString(in);
        metaCacheName = U.readString(in);
        dataCacheName = U.readString(in);
        blockSize = in.readInt();
        prefetchBlocks = in.readInt();
        streamBufSize = in.readInt();
        perNodeBatchSize = in.readInt();
        perNodeParallelBatchCnt = in.readInt();
        dfltMode = IgfsMode.fromOrdinal(in.readByte());
        pathModes = U.readMap(in);
        maxTaskRangeLen = in.readLong();
        fragmentizerConcurrentFiles = in.readInt();
        fragmentizerEnabled = in.readBoolean();
        fragmentizerThrottlingBlockLen = in.readLong();
        fragmentizerThrottlingDelay = in.readLong();
        ipcEndpointCfg = U.readString(in);
        ipcEndpointEnabled = in.readBoolean();
        mgmtPort = in.readInt();
        seqReadsBeforePrefetch = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorIgfsConfiguration.class, this);
    }
}
