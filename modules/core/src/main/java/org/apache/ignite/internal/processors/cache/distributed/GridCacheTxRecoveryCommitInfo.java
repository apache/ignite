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
import org.apache.ignite.internal.processors.cache.consistentcut.ConsistentCutVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * It describes info received from remote nodes during transaction recovery procedure.
 */
public class GridCacheTxRecoveryCommitInfo implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 183;

    /** */
    private static final GridCacheTxRecoveryCommitInfo NO_COMMIT = new GridCacheTxRecoveryCommitInfo(false, null);

    /**
     * If {@code true} then transaction was prepared and can be committed.
     */
    @GridToStringInclude
    private boolean commit;

    /**
     * Consistent Cut version with that this transaction was signed for commit.
     */
    @GridToStringInclude
    private @Nullable ConsistentCutVersion cutVer;

    /**
     * Empty constructor required by {@link Externalizable}
     */
    public GridCacheTxRecoveryCommitInfo() {
        // No-op.
    }

    /** */
    public GridCacheTxRecoveryCommitInfo(boolean commit, @Nullable ConsistentCutVersion cutVer) {
        this.commit = commit;
        this.cutVer = cutVer;
    }

    /** */
    public static GridCacheTxRecoveryCommitInfo noCommit() {
        return NO_COMMIT;
    }

    /** */
    public boolean commit() {
        return commit;
    }

    /** */
    public ConsistentCutVersion cutVer() {
        return cutVer;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeBoolean("commit", commit))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage("ver", cutVer))
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

        switch (reader.state()) {
            case 0:
                commit = reader.readBoolean("commit");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                cutVer = reader.readMessage("ver");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(GridCacheTxRecoveryCommitInfo.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheTxRecoveryCommitInfo.class, this);
    }
}
