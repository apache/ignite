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

package org.apache.ignite.internal.processors.cache.version;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Extended cache version which also has additional DR version.
 */
public class GridCacheVersionEx extends GridCacheVersion {
    /** */
    private static final long serialVersionUID = 0L;

    /** DR version. */
    private GridCacheVersion drVer;

    /**
     * {@link Externalizable} support.
     */
    public GridCacheVersionEx() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param topVer Topology version.
     * @param globalTime Global time.
     * @param order Order.
     * @param nodeOrder Node order.
     * @param dataCenterId Data center ID.
     * @param drVer DR version.
     */
    public GridCacheVersionEx(int topVer, long globalTime, long order, int nodeOrder, byte dataCenterId,
        GridCacheVersion drVer) {
        super(topVer, globalTime, order, nodeOrder, dataCenterId);

        assert drVer != null && !(drVer instanceof GridCacheVersionEx); // DR version can only be plain here.

        this.drVer = drVer;
    }

    /**
     * Constructor.
     *
     * @param topVer Topology version.
     * @param nodeOrderDrId Node order and DR ID.
     * @param globalTime Globally adjusted time.
     * @param order Version order.
     * @param drVer DR version.
     */
    public GridCacheVersionEx(int topVer, int nodeOrderDrId, long globalTime, long order, GridCacheVersion drVer) {
        super(topVer, nodeOrderDrId, globalTime, order);

        assert drVer != null && !(drVer instanceof GridCacheVersionEx); // DR version can only be plain here.

        this.drVer = drVer;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion conflictVersion() {
        return drVer;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 104;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 5;
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
            case 4:
                if (!writer.writeMessage("drVer", drVer))
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
            case 4:
                drVer = reader.readMessage("drVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridCacheVersionEx.class);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        super.readExternal(in);

        drVer = new GridCacheVersion();

        drVer.readExternal(in);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        drVer.writeExternal(out);
    }
}