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
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Result object for cache rebalance job.
 */
public class VisorCacheRebalanceCollectorJobResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Rebalance percent. */
    private double rebalance;

    /** Node baseline state. */
    private VisorNodeBaselineStatus baseline;

    /**
     * Default constructor.
     */
    public VisorCacheRebalanceCollectorJobResult() {
        // No-op.
    }

    /**
     * @return Rebalance progress.
     */
    public double getRebalance() {
        return rebalance;
    }

    /**
     * @param rebalance Rebalance progress.
     */
    public void setRebalance(double rebalance) {
        this.rebalance = rebalance;
    }

    /**
     * @return Node baseline status.
     */
    public VisorNodeBaselineStatus getBaseline() {
        return baseline;
    }

    /**
     * @param baseline Node baseline status.
     */
    public void setBaseline(VisorNodeBaselineStatus baseline) {
        this.baseline = baseline;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeDouble(rebalance);
        U.writeEnum(out, baseline);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        rebalance = in.readDouble();
        baseline = VisorNodeBaselineStatus.fromOrdinal(in.readByte());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheRebalanceCollectorJobResult.class, this);
    }
}
