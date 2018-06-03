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
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Data transfer object for memory configuration.
 */
public class VisorMemoryPolicyConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Unique name of DataRegion. */
    private String name;

    /** Maximum memory region size defined by this memory policy. */
    private long maxSize;

    /** Initial memory region size defined by this memory policy. */
    private long initSize;

    /** Path for memory mapped file. */
    private String swapFilePath;

    /** An algorithm for memory pages eviction. */
    private DataPageEvictionMode pageEvictionMode;

    /**
     * A threshold for memory pages eviction initiation. For instance, if the threshold is 0.9 it means that the page
     * memory will start the eviction only after 90% memory region (defined by this policy) is occupied.
     */
    private double evictionThreshold;

    /** Minimum number of empty pages in reuse lists. */
    private int emptyPagesPoolSize;

    /**
     * Default constructor.
     */
    public VisorMemoryPolicyConfiguration() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param plc Memory policy configuration.
     */
    public VisorMemoryPolicyConfiguration(DataRegionConfiguration plc) {
        assert plc != null;

        name = plc.getName();
        maxSize = plc.getMaxSize();
        initSize = plc.getInitialSize();
        swapFilePath = plc.getSwapPath();
        pageEvictionMode = plc.getPageEvictionMode();
        evictionThreshold = plc.getEvictionThreshold();
        emptyPagesPoolSize = plc.getEmptyPagesPoolSize();
    }

    /**
     * Unique name of DataRegion.
     */
    public String getName() {
        return name;
    }

    /**
     * Maximum memory region size defined by this memory policy.
     */
    public long getMaxSize() {
        return maxSize;
    }

    /**
     * Initial memory region size defined by this memory policy.
     */
    public long getInitialSize() {
        return initSize;
    }

    /**
     * @return Path for memory mapped file.
     */
    public String getSwapFilePath() {
        return swapFilePath;
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

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, name);
        out.writeLong(initSize);
        out.writeLong(maxSize);
        U.writeString(out, swapFilePath);
        U.writeEnum(out, pageEvictionMode);
        out.writeDouble(evictionThreshold);
        out.writeInt(emptyPagesPoolSize);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        name = U.readString(in);
        initSize = in.readLong();
        maxSize = in.readLong();
        swapFilePath = U.readString(in);
        pageEvictionMode = DataPageEvictionMode.fromOrdinal(in.readByte());
        evictionThreshold = in.readDouble();
        emptyPagesPoolSize = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorMemoryPolicyConfiguration.class, this);
    }
}
