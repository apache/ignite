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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.h2.value.Value;

import static org.h2.util.StringUtils.convertBytesToHex;

/**
 * H2 Geometry.
 */
public class GridH2Geometry extends GridH2ValueMessage {
    /** */
    private static final Method GEOMETRY_FROM_BYTES;

    /**
     * Initialize field.
     */
    static {
        try {
            GEOMETRY_FROM_BYTES = Class.forName("org.h2.value.ValueGeometry").getMethod("get", byte[].class);
        }
        catch (NoSuchMethodException | ClassNotFoundException ignored) {
            throw new IllegalStateException("Check H2 version in classpath.");
        }
    }

    /** */
    private byte[] b;

    /**
     *
     */
    public GridH2Geometry() {
        // No-op.
    }

    /**
     * @param val Value.
     */
    public GridH2Geometry(Value val) {
        assert val.getType() == Value.GEOMETRY : val.getType();

        b = val.getBytesNoCopy();
    }

    /** {@inheritDoc} */
    @Override public Value value(GridKernalContext ctx) {
        try {
            return (Value)GEOMETRY_FROM_BYTES.invoke(null, new Object[]{b});
        }
        catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
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
                if (!writer.writeByteArray("b", b))
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
                b = reader.readByteArray("b");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridH2Geometry.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -21;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "g_" + convertBytesToHex(b);
    }
}
