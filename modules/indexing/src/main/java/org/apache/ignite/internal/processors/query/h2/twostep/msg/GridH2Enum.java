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

package org.apache.ignite.internal.processors.query.h2.twostep.msg;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ValueEnum;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ValueEnumCache;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.h2.value.Value;

import java.nio.ByteBuffer;


/**
 * H2 Enum message
 */
public class GridH2Enum  extends GridH2ValueMessage {
    /** */
    private int typeId;

    /** */
    private String clsName;

    /** */
    private int ordinal;

    /** */
    public GridH2Enum() {
        //no-op
    }

    /** */
    public GridH2Enum(Value val) {
        assert val instanceof GridH2ValueEnum;
        typeId = val.getType();
        ordinal = val.getInt();

        GridH2ValueEnum value = (GridH2ValueEnum)val;
        clsName = value.getEnumClass().getName();
    }

    /** {@inheritDoc} */
    @Override public Value value(GridKernalContext ctx) throws IgniteCheckedException {
        //type, class name and ordinal are read. need to construct proper Value
        Class<?> cls;
        try {
            cls = U.resolveClassLoader(ctx.config()).loadClass(clsName);
        }
        catch (ClassNotFoundException ex) {
            throw new IgniteCheckedException(ex);
        }
        return GridH2ValueEnumCache.get(typeId, cls, ordinal);
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
            case 0:
                if (!writer.writeInt("t", typeId))
                    return false;

                writer.incrementState();
            case 1:
                if (!writer.writeString("c", clsName))
                    return false;

                writer.incrementState();
            case 2:
                if (!writer.writeInt("o", ordinal))
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
            case 0:
                typeId = reader.readInt("t");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
            case 1:
                clsName = reader.readString("c");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
            case 2:
                ordinal = reader.readInt("o");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(GridH2Enum.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -49;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "e_" + typeId + "_" + clsName + "_" + ordinal;
    }
}
