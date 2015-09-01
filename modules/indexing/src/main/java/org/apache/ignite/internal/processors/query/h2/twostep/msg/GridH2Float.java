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

import java.nio.ByteBuffer;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.h2.value.Value;
import org.h2.value.ValueFloat;

/**
 * H2 Float.
 */
public class GridH2Float extends GridH2ValueMessage {
    /** */
    private float x;

    /**
     *
     */
    public GridH2Float() {
        // No-op.
    }

    /**
     * @param val Value.
     */
    public GridH2Float(Value val) {
        assert val.getType() == Value.FLOAT : val.getType();

        x = val.getFloat();
    }

    /** {@inheritDoc} */
    @Override public Value value(GridKernalContext ctx) {
        return ValueFloat.get(x);
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
                if (!writer.writeFloat("x", x))
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
                x = reader.readFloat("x");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -12;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }
}