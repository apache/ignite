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

package org.apache.ignite.internal;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public final class ExchangeInfo extends IgniteDiagnosticMessage.DiagnosticBaseInfo {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(value = 0, method = "topologyVersion")
    private AffinityTopologyVersion topVer;

    /**
     * Empty constructor required by {@link GridIoMessageFactory}.
     */
    public ExchangeInfo() {
        // No-op.
    }

    /**
     * @param topVer Exchange version.
     */
    public ExchangeInfo(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /** */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** */
    public void topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -62;
    }

    /** {@inheritDoc} */
    @Override public void appendInfo(StringBuilder sb, GridKernalContext ctx) {
        sb.append(U.nl());

        List<GridDhtPartitionsExchangeFuture> futs = ctx.cache().context().exchange().exchangeFutures();

        for (GridDhtPartitionsExchangeFuture fut : futs) {
            if (topVer.equals(fut.initialVersion())) {
                sb.append("Exchange future: ").append(fut);

                return;
            }
        }

        sb.append("Failed to find exchange future: ").append(topVer);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(topVer);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        topVer = (AffinityTopologyVersion)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ExchangeInfo that = (ExchangeInfo)o;

        return Objects.equals(topVer, that.topVer);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(getClass(), topVer);
    }
}
