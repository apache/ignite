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

package org.apache.ignite.internal.processors.cache;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUpdateVersionAware;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersionAware;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_HINTS_BIT_OFF;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_HINTS_MASK;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_OP_COUNTER_MASK;

/**
 *
 */
public class GridCacheMvccEntryInfo extends GridCacheEntryInfo implements MvccVersionAware, MvccUpdateVersionAware {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int SIZE_OVERHEAD = 4 * 8 /* long */ + 2 * 4 /* int */;

    /** */
    private long mvccCrdVer;

    /** */
    private long mvccCntr;

    /** */
    private int mvccOpCntr;

    /** */
    private long newMvccCrdVer;

    /** */
    private long newMvccCntr;

    /** */
    private int newMvccOpCntr;

    /** {@inheritDoc} */
    @Override public long newMvccCoordinatorVersion() {
        return newMvccCrdVer;
    }

    /** {@inheritDoc} */
    @Override public long newMvccCounter() {
        return newMvccCntr;
    }

    /** {@inheritDoc} */
    @Override public int newMvccOperationCounter() {
        return newMvccOpCntr & MVCC_OP_COUNTER_MASK;
    }

    /** {@inheritDoc} */
    @Override public byte newMvccTxState() {
        return (byte)(newMvccOpCntr >>> MVCC_HINTS_BIT_OFF);
    }

    /** {@inheritDoc} */
    @Override public long mvccCoordinatorVersion() {
        return mvccCrdVer;
    }

    /** {@inheritDoc} */
    @Override public long mvccCounter() {
        return mvccCntr;
    }

    /** {@inheritDoc} */
    @Override public int mvccOperationCounter() {
        return mvccOpCntr & MVCC_OP_COUNTER_MASK;
    }

    /** {@inheritDoc} */
    @Override public byte mvccTxState() {
        return (byte)(mvccOpCntr >>> MVCC_HINTS_BIT_OFF);
    }

    /**
     * @param mvccTxState Mvcc version Tx state hint.
     */
    public void mvccTxState(byte mvccTxState) {
        mvccOpCntr = (mvccOpCntr & ~MVCC_HINTS_MASK) | ((int)mvccTxState << MVCC_HINTS_BIT_OFF);
    }

    /**
     * @param newMvccTxState New mvcc version Tx state hint.
     */
    public void newMvccTxState(byte newMvccTxState) {
        newMvccOpCntr = (newMvccOpCntr & ~MVCC_HINTS_MASK) | ((int)newMvccTxState << MVCC_HINTS_BIT_OFF);
    }

    /** {@inheritDoc} */
    @Override public void newMvccVersion(long crd, long cntr, int opCntr) {
        newMvccCrdVer = crd;
        newMvccCntr = cntr;
        newMvccOpCntr = opCntr;
    }

    /** {@inheritDoc} */
    @Override public void mvccVersion(long crd, long cntr, int opCntr) {
        mvccCrdVer = crd;
        mvccCntr = cntr;
        mvccOpCntr = opCntr;
    }

    /** {@inheritDoc} */
    @Override public int marshalledSize(CacheObjectContext ctx) throws IgniteCheckedException {
        return SIZE_OVERHEAD + super.marshalledSize(ctx);
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
            case 6:
                if (!writer.writeLong("mvccCntr", mvccCntr))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeLong("mvccCrdVer", mvccCrdVer))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeInt("mvccOpCntr", mvccOpCntr))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeLong("newMvccCntr", newMvccCntr))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeLong("newMvccCrdVer", newMvccCrdVer))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeInt("newMvccOpCntr", newMvccOpCntr))
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
            case 6:
                mvccCntr = reader.readLong("mvccCntr");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                mvccCrdVer = reader.readLong("mvccCrdVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                mvccOpCntr = reader.readInt("mvccOpCntr");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                newMvccCntr = reader.readLong("newMvccCntr");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                newMvccCrdVer = reader.readLong("newMvccCrdVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                newMvccOpCntr = reader.readInt("newMvccOpCntr");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridCacheMvccEntryInfo.class);
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 12;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 143;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheMvccEntryInfo.class, this);
    }
}
