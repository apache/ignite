/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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

    /** Baseline auto adjust enable flag. */
    private Boolean autoAdjustEnabled;

    /** Awaiting time of baseline auto adjust after last topology event in ms. */
    private Long autoAdjustAwaitingTime;

    /**
     * Default constructor.
     */
    public VisorBaselineTaskArg() {
        // No-op.
    }

    /**
     * This constructor is required by Web Console.
     * Do not remove or change signature.
     *
     * @param topVer Topology version.
     * @param consistentIds Consistent ids.
     */
    public VisorBaselineTaskArg(
        VisorBaselineOperation op,
        long topVer,
        List<String> consistentIds
    ) {
        this(op, topVer, consistentIds, null, null);
    }

    /**
     * @param topVer Topology version.
     * @param consistentIds Consistent ids.
     * @param autoAdjustEnabled Baseline auto adjust enable flag.
     * @param autoAdjustAwaitingTime Await time of baseline auto adjust after last topology event in ms.
     */
    public VisorBaselineTaskArg(
        VisorBaselineOperation op,
        long topVer,
        List<String> consistentIds,
        Boolean autoAdjustEnabled,
        Long autoAdjustAwaitingTime
    ) {
        this.op = op;
        this.topVer = topVer;
        this.consistentIds = consistentIds;
        this.autoAdjustEnabled = autoAdjustEnabled;
        this.autoAdjustAwaitingTime = autoAdjustAwaitingTime;
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
     * @return Baseline auto adjust enable flag.
     */
    public Boolean isAutoAdjustEnabled() {
        return autoAdjustEnabled;
    }

    /**
     * @return Await time of baseline auto adjust after last topology event in ms.
     */
    public Long getAutoAdjustAwaitingTime() {
        return autoAdjustAwaitingTime;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeEnum(out, op);
        out.writeLong(topVer);
        U.writeCollection(out, consistentIds);
        out.writeObject(autoAdjustEnabled);
        out.writeObject(autoAdjustAwaitingTime);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        op = VisorBaselineOperation.fromOrdinal(in.readByte());
        topVer = in.readLong();
        consistentIds = U.readList(in);

        if (protoVer > V1) {
            autoAdjustEnabled = (Boolean)in.readObject();
            autoAdjustAwaitingTime = (Long)in.readObject();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorBaselineTaskArg.class, this);
    }
}
