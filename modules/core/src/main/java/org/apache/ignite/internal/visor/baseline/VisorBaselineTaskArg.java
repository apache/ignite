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

package org.apache.ignite.internal.visor.baseline;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Argument for {@link VisorBaselineTask}.
 */
public class VisorBaselineTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private VisorBaselineOperation op;

    /** */
    private long topVer;

    /** */
    private List<String> consistentIds;

    /** */
    private VisorBaselineAutoAdjustSettings autoAdjustSettings;

    /**
     * Default constructor.
     */
    public VisorBaselineTaskArg() {
        // No-op.
    }

    /**
     * @param topVer Topology version.
     * @param consistentIds Consistent ids.
     * @param autoAdjustSettings Baseline autoadjustment settings.
     */
    public VisorBaselineTaskArg(
        VisorBaselineOperation op,
        long topVer,
        List<String> consistentIds,
        VisorBaselineAutoAdjustSettings autoAdjustSettings
    ) {
        this.op = op;
        this.topVer = topVer;
        this.consistentIds = consistentIds;
        this.autoAdjustSettings = autoAdjustSettings;
    }

    /**
     * @return Base line operation.
     */
    public VisorBaselineOperation getOperation() {
        return op;
    }

    /**
     * @return Topology version.
     */
    public long getTopologyVersion() {
        return topVer;
    }

    /**
     * @return Consistent IDs.
     */
    public List<String> getConsistentIds() {
        return consistentIds;
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V2;
    }

    /**
     * @return Baseline autoadjustment settings.
     */
    public VisorBaselineAutoAdjustSettings getAutoAdjustSettings() {
        return autoAdjustSettings;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeEnum(out, op);
        out.writeLong(topVer);
        U.writeCollection(out, consistentIds);
        out.writeObject(autoAdjustSettings);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        op = VisorBaselineOperation.fromOrdinal(in.readByte());
        topVer = in.readLong();
        consistentIds = U.readList(in);

        if (protoVer > V1)
            autoAdjustSettings = (VisorBaselineAutoAdjustSettings)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorBaselineTaskArg.class, this);
    }
}
