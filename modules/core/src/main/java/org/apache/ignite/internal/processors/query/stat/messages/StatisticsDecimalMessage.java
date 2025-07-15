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

package org.apache.ignite.internal.processors.query.stat.messages;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * H2 Decimal.
 */
public class StatisticsDecimalMessage implements Message, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 184;

    /** */
    private int scale;

    /** */
    private byte[] b;

    /**
     *
     */
    public StatisticsDecimalMessage() {
        // No-op.
    }

    /**
     * @param val Value.
     */
    public StatisticsDecimalMessage(BigDecimal val) {
        if (val == null) {
            scale = 0;
            b = null;
        }
        else {
            scale = val.scale();
            b = val.unscaledValue().toByteArray();
        }
    }

    /**
     * @return Decimal value.
     */
    public BigDecimal value() {
        if (b == null && scale == 0)
            return null;

        return new BigDecimal(new BigInteger(b), scale);
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeByteArray(b))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeInt(scale))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        switch (reader.state()) {
            case 0:
                b = reader.readByteArray();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                scale = reader.readInt();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return Objects.toString(value());
    }
}
