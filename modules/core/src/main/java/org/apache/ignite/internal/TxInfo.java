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

import java.util.Objects;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public final class TxInfo extends IgniteDiagnosticMessage.DiagnosticBaseInfo {
    /** */
    @Order(value = 0, method = "dhtVersion")
    private GridCacheVersion dhtVer;

    /** */
    @Order(value = 1, method = "nearVersion")
    private GridCacheVersion nearVer;

    /**
     * Empty constructor required by {@link GridIoMessageFactory}.
     */
    public TxInfo() {
        // No-op.
    }

    /**
     * @param dhtVer  Tx dht version.
     * @param nearVer Tx near version.
     */
    public TxInfo(GridCacheVersion dhtVer, GridCacheVersion nearVer) {
        this.dhtVer = dhtVer;
        this.nearVer = nearVer;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -64;
    }

    /** */
    public GridCacheVersion dhtVersion() {
        return dhtVer;
    }

    /** */
    public void dhtVersion(GridCacheVersion dhtVer) {
        this.dhtVer = dhtVer;
    }

    /** */
    public GridCacheVersion nearVersion() {
        return nearVer;
    }

    /** */
    public void nearVersion(GridCacheVersion nearVer) {
        this.nearVer = nearVer;
    }

    /** {@inheritDoc} */
    @Override public void appendInfo(StringBuilder sb, GridKernalContext ctx) {
        sb.append(U.nl())
            .append("Related transactions [dhtVer=").append(dhtVer)
            .append(", nearVer=").append(nearVer).append("]: ");

        boolean found = false;

        for (IgniteInternalTx tx : ctx.cache().context().tm().activeTransactions()) {
            if (dhtVer.equals(tx.xidVersion()) || nearVer.equals(tx.nearXidVersion())) {
                sb.append(U.nl())
                    .append("    ")
                    .append(tx.getClass().getSimpleName())
                    .append(" [ver=").append(tx.xidVersion())
                    .append(", nearVer=").append(tx.nearXidVersion())
                    .append(", topVer=").append(tx.topologyVersion())
                    .append(", state=").append(tx.state())
                    .append(", fullTx=").append(tx).append(']');

                found = true;
            }
        }

        if (!found)
            sb.append(U.nl()).append("Failed to find related transactions.");
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        TxInfo txInfo = (TxInfo)o;

        return Objects.equals(dhtVer, txInfo.dhtVer) && Objects.equals(nearVer, txInfo.nearVer);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(getClass(), nearVer, dhtVer);
    }
}
