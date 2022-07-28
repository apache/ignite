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

package org.apache.ignite.internal.processors.cache.distributed;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.consistentcut.ConsistentCutVersion;
import org.apache.ignite.internal.processors.cache.consistentcut.ConsistentCutVersionAware;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxState;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxStateAware;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Transactions recovery check response.
 */
public class GridCacheTxRecoveryResponse extends GridDistributedBaseMessage implements IgniteTxStateAware, ConsistentCutVersionAware {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private IgniteUuid futId;

    /** Mini future ID. */
    private IgniteUuid miniId;

    /** Transient TX state. */
    @GridDirectTransient
    private IgniteTxState txState;

    /** Version of the latest known Consistent Cut on local node. */
    @Nullable private ConsistentCutVersion latestCutVer;

    /**
     * Version of the latest Consistent Cut AFTER which this transaction committed.
     * Sets on near node to notify other nodes in 2PC algorithm.
     */
    private GridCacheTxRecoveryCommitInfo commit;

    /**
     * Empty constructor required by {@link Externalizable}
     */
    public GridCacheTxRecoveryResponse() {
        // No-op.
    }

    /**
     * @param txId Transaction ID.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param addDepInfo Deployment info flag.
     * @param commit Commit info on local node for specified transaction.
     * @param latestCutVer The latest known Consistent Cut on sender node.
     */
    public GridCacheTxRecoveryResponse(GridCacheVersion txId,
        IgniteUuid futId,
        IgniteUuid miniId,
        boolean addDepInfo,
        GridCacheTxRecoveryCommitInfo commit,
        @Nullable ConsistentCutVersion latestCutVer) {
        super(txId, 0, addDepInfo);

        this.futId = futId;
        this.miniId = miniId;

        this.addDepInfo = addDepInfo;

        this.commit = commit;
        this.latestCutVer = latestCutVer;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Mini future ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @return Commit info on local node for specified transaction.
     */
    public GridCacheTxRecoveryCommitInfo commit() {
        return commit;
    }

    /** {@inheritDoc} */
    @Override public ConsistentCutVersion latestCutVersion() {
        return latestCutVer;
    }

    /** {@inheritDoc} */
    @Override public IgniteTxState txState() {
        return txState;
    }

    /** {@inheritDoc} */
    @Override public void txState(IgniteTxState txState) {
        this.txState = txState;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger messageLogger(GridCacheSharedContext ctx) {
        return ctx.txRecoveryMessageLogger();
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 8:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeIgniteUuid("miniId", miniId))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeMessage("commit", commit))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeMessage("latestCutVer", latestCutVer))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 8:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                miniId = reader.readIgniteUuid("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                commit = reader.readMessage("commit");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                latestCutVer = reader.readMessage("latestCutVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(GridCacheTxRecoveryResponse.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 17;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 12;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheTxRecoveryResponse.class, this, "super", super.toString());
    }
}
