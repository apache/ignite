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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class GridCacheMvccEntryInfo extends GridCacheEntryInfo {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long mvccCrdVer;

    /** */
    private long mvccCntr;

    /** {@inheritDoc} */
    @Override public void mvccCoordinatorVersion(long mvccCrdVer) {
        this.mvccCrdVer = mvccCrdVer;
    }

    /** {@inheritDoc} */
    @Override public void mvccCounter(long mvccCntr) {
        this.mvccCntr = mvccCntr;
    }

    /** {@inheritDoc} */
    @Override public long coordinatorVersion() {
        return mvccCrdVer;
    }

    /** {@inheritDoc} */
    @Override public long counter() {
        return mvccCntr;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 8;
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

        }

        return reader.afterMessageRead(GridCacheMvccEntryInfo.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 138;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheMvccEntryInfo.class, this);
    }
}
