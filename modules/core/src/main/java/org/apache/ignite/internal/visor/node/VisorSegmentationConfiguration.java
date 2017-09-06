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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.apache.ignite.plugin.segmentation.SegmentationPolicy;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactArray;

/**
 * Data transfer object for node segmentation configuration properties.
 */
public class VisorSegmentationConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Segmentation policy. */
    private SegmentationPolicy plc;

    /** Segmentation resolvers. */
    private String resolvers;

    /** Frequency of network segment check by discovery manager. */
    private long checkFreq;

    /** Whether or not node should wait for correct segment on start. */
    private boolean waitOnStart;

    /** Whether or not all resolvers should succeed for node to be in correct segment. */
    private boolean allResolversPassReq;

    /** Segmentation resolve attempts count. */
    private int segResolveAttempts;

    /**
     * Default constructor.
     */
    public VisorSegmentationConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for node segmentation configuration properties.
     *
     * @param c Grid configuration.
     */
    public VisorSegmentationConfiguration(IgniteConfiguration c) {
        plc = c.getSegmentationPolicy();
        resolvers = compactArray(c.getSegmentationResolvers());
        checkFreq = c.getSegmentCheckFrequency();
        waitOnStart = c.isWaitForSegmentOnStart();
        allResolversPassReq = c.isAllSegmentationResolversPassRequired();
        segResolveAttempts = c.getSegmentationResolveAttempts();
    }

    /**
     * @return Segmentation policy.
     */
    public SegmentationPolicy getPolicy() {
        return plc;
    }

    /**
     * @return Segmentation resolvers.
     */
    @Nullable public String getResolvers() {
        return resolvers;
    }

    /**
     * @return Frequency of network segment check by discovery manager.
     */
    public long getCheckFrequency() {
        return checkFreq;
    }

    /**
     * @return Whether or not node should wait for correct segment on start.
     */
    public boolean isWaitOnStart() {
        return waitOnStart;
    }

    /**
     * @return Whether or not all resolvers should succeed for node to be in correct segment.
     */
    public boolean isAllSegmentationResolversPassRequired() {
        return allResolversPassReq;
    }

    /**
     * @return Segmentation resolve attempts.
     */
    public int getSegmentationResolveAttempts() {
        return segResolveAttempts;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeEnum(out, plc);
        U.writeString(out, resolvers);
        out.writeLong(checkFreq);
        out.writeBoolean(waitOnStart);
        out.writeBoolean(allResolversPassReq);
        out.writeInt(segResolveAttempts);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        plc = SegmentationPolicy.fromOrdinal(in.readByte());
        resolvers = U.readString(in);
        checkFreq = in.readLong();
        waitOnStart = in.readBoolean();
        allResolversPassReq = in.readBoolean();
        segResolveAttempts = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorSegmentationConfiguration.class, this);
    }
}
