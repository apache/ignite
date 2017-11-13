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
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Data transfer object for data region configuration.
 */
public class VisorDataRegionConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Unique name of DataRegion. */
    private String name;

    /** Initial memory region size defined by this memory policy. */
    private long initSize;

    /** Maximum memory region size defined by this memory policy. */
    private long maxSize;

    /** Path for memory mapped file. */
    private String swapPath;

    /** An algorithm for memory pages eviction. */
    private DataPageEvictionMode pageEvictionMode;

    /** A threshold for memory pages eviction initiation. */
    private double evictionThreshold;

    /** Minimum number of empty pages in reuse lists. */
    private int emptyPagesPoolSize;

    /** Enable memory metrics collection for this data region. */
    private boolean metricsEnabled;

    /** Number of sub-intervals. */
    private int metricsSubIntervalCount;

    /** Time interval over which allocation rate is calculated. */
    private long metricsRateTimeInterval;

    /** Enable Ignite Native Persistence. */
    private boolean persistenceEnabled;

    /** Temporary buffer size for checkpoints in bytes. */
    private long checkpointPageBufSize;

    /**
     * Default constructor.
     */
    public VisorDataRegionConfiguration() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param plc Data region configuration.
     */
    public VisorDataRegionConfiguration(DataRegionConfiguration plc) {
        assert plc != null;

        name = plc.getName();
        initSize = plc.getInitialSize();
        maxSize = plc.getMaxSize();
        swapPath = plc.getSwapPath();
        pageEvictionMode = plc.getPageEvictionMode();
        evictionThreshold = plc.getEvictionThreshold();
        emptyPagesPoolSize = plc.getEmptyPagesPoolSize();
        metricsEnabled = plc.isMetricsEnabled();
        metricsSubIntervalCount = plc.getMetricsSubIntervalCount();
        metricsRateTimeInterval = plc.getMetricsRateTimeInterval();
        persistenceEnabled = plc.isPersistenceEnabled();
        checkpointPageBufSize = plc.getCheckpointPageBufferSize();
    }

    /**
     * @param regCfgs Array of data region configurations.
     * @return Collection of DTO objects.
     */
    public static List<VisorDataRegionConfiguration> from(DataRegionConfiguration[] regCfgs) {
        List<VisorDataRegionConfiguration> res = new ArrayList<>();

        if (regCfgs != null) {
            for (DataRegionConfiguration plc: regCfgs)
                res.add(new VisorDataRegionConfiguration(plc));
        }

        return res;
    }

    /**
     * @return Unique name of DataRegion.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Maximum memory region size defined by this memory policy.
     */
    public long getMaxSize() {
        return maxSize;
    }

    /**
     * @return Initial memory region size defined by this memory policy.
     */
    public long getInitialSize() {
        return initSize;
    }

    /**
     * @return Path for memory mapped file.
     */
    public String getSwapPath() {
        return swapPath;
    }

    /**
     * @return Memory pages eviction algorithm. {@link DataPageEvictionMode#DISABLED} used by default.
     */
    public DataPageEvictionMode getPageEvictionMode() {
        return pageEvictionMode;
    }

    /**
     * @return Memory pages eviction threshold.
     */
    public double getEvictionThreshold() {
        return evictionThreshold;
    }

    /**
     * @return Minimum number of empty pages in reuse list.
     */
    public int getEmptyPagesPoolSize() {
        return emptyPagesPoolSize;
    }

    /**
     * @return Metrics enabled flag.
     */
    public boolean isMetricsEnabled() {
        return metricsEnabled;
    }

    /**
     * @return Number of sub intervals.
     */
    public int getMetricsSubIntervalCount() {
        return metricsSubIntervalCount;
    }

    /**
     * @return Time interval over which allocation rate is calculated.
     */
    public long getMetricsRateTimeInterval() {
        return metricsRateTimeInterval;
    }

    /**
     * @return Persistence enabled flag.
     */
    public boolean isPersistenceEnabled() {
        return persistenceEnabled;
    }

    /**
     * @return Amount of memory allocated for a checkpoint temporary buffer in bytes.
     */
    public long getCheckpointPageBufferSize() {
        return checkpointPageBufSize;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, name);
        out.writeLong(initSize);
        out.writeLong(maxSize);
        U.writeString(out, swapPath);
        U.writeEnum(out, pageEvictionMode);
        out.writeDouble(evictionThreshold);
        out.writeInt(emptyPagesPoolSize);
        out.writeBoolean(metricsEnabled);
        out.writeInt(metricsSubIntervalCount);
        out.writeLong(metricsRateTimeInterval);
        out.writeBoolean(persistenceEnabled);
        out.writeLong(checkpointPageBufSize);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        name = U.readString(in);
        initSize = in.readLong();
        maxSize = in.readLong();
        swapPath = U.readString(in);
        pageEvictionMode = DataPageEvictionMode.fromOrdinal(in.readByte());
        evictionThreshold = in.readDouble();
        emptyPagesPoolSize = in.readInt();
        metricsEnabled = in.readBoolean();
        metricsSubIntervalCount = in.readInt();
        metricsRateTimeInterval = in.readLong();
        persistenceEnabled = in.readBoolean();
        checkpointPageBufSize = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDataRegionConfiguration.class, this);
    }
}
