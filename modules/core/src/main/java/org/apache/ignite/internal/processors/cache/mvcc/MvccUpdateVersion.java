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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class MvccUpdateVersion implements Comparable<MvccUpdateVersion>, Message {
    /** */
    public static final long COUNTER_NA = 0L;

    /** */
    private long topVer;

    /** */
    private long cntr;

    /**
     *
     */
    public MvccUpdateVersion() {
        // No-op.
    }

    /**
     * @param topVer Topology version.
     * @param cntr Coordinator counter.
     */
    public MvccUpdateVersion(long topVer, long cntr) {
        assert topVer > 0 : topVer;
        assert cntr != COUNTER_NA;

        this.topVer = topVer;
        this.cntr = cntr;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull MvccUpdateVersion other) {
        int cmp = Long.compare(topVer, other.topVer);

        if (cmp != 0)
            return cmp;

        return Long.compare(cntr, other.cntr);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        MvccUpdateVersion that = (MvccUpdateVersion) o;

        return topVer == that.topVer && cntr == that.cntr;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = (int) (topVer ^ (topVer >>> 32));

        res = 31 * res + (int) (cntr ^ (cntr >>> 32));

        return res;
    }

    /**
     * @return Coordinators topology version.
     */
    public long topologyVersion() {
        return topVer;
    }

    /**
     * @return Counters.
     */
    public long counter() {
        return cntr;
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
                if (!writer.writeLong("cntr", cntr))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeLong("topVer", topVer))
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
                cntr = reader.readLong("cntr");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                topVer = reader.readLong("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(MvccUpdateVersion.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 135;
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
        return S.toString(MvccUpdateVersion.class, this);
    }
}
