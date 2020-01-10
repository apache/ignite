/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.message;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteCodeGeneratingFail;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
@IgniteCodeGeneratingFail
public class QueryBatchMessage implements MarshalableMessage {
    private RelDataType dataType;

    byte[] types;

    private List<Object[]> rows;

    private int size;

    @Override public void prepareMarshal(Marshaller marshaller) throws IgniteCheckedException {
        size = rows.size();

        types = new byte[dataType.getFieldCount()];

        List<RelDataTypeField> fieldList = dataType.getFieldList();

        for (int i = 0; i < fieldList.size(); i++) {
            RelDataType type = fieldList.get(i).getType();
            switch (type.getSqlTypeName()) {
                case BOOLEAN:
                    break;
                case TINYINT:
                    break;
                case SMALLINT:
                    break;
                case INTEGER:
                    break;
                case BIGINT:
                    break;
                case DECIMAL:
                    break;
                case FLOAT:
                    break;
                case REAL:
                    break;
                case DOUBLE:
                    break;
                case DATE:
                    break;
                case TIME:
                    break;
                case TIME_WITH_LOCAL_TIME_ZONE:
                    break;
                case TIMESTAMP:
                    break;
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    break;
                case INTERVAL_YEAR:
                    break;
                case INTERVAL_YEAR_MONTH:
                    break;
                case INTERVAL_MONTH:
                    break;
                case INTERVAL_DAY:
                    break;
                case INTERVAL_DAY_HOUR:
                    break;
                case INTERVAL_DAY_MINUTE:
                    break;
                case INTERVAL_DAY_SECOND:
                    break;
                case INTERVAL_HOUR:
                    break;
                case INTERVAL_HOUR_MINUTE:
                    break;
                case INTERVAL_HOUR_SECOND:
                    break;
                case INTERVAL_MINUTE:
                    break;
                case INTERVAL_MINUTE_SECOND:
                    break;
                case INTERVAL_SECOND:
                    break;
                case CHAR:
                    break;
                case VARCHAR:
                    break;
                case BINARY:
                    break;
                case VARBINARY:
                    break;
                case NULL:
                    break;
                case ANY:
                    break;
                case SYMBOL:
                    break;
                case MULTISET:
                    break;
                case ARRAY:
                    break;
                case MAP:
                    break;
                case DISTINCT:
                    break;
                case STRUCTURED:
                    break;
                case ROW:
                    break;
                case OTHER:
                    break;
                case CURSOR:
                    break;
                case COLUMN_LIST:
                    break;
                case DYNAMIC_STAR:
                    break;
                case GEOMETRY:
                    break;
            }
        }
    }

    @Override public void prepareUnmarshal(Marshaller marshaller, ClassLoader loader) throws IgniteCheckedException {

    }

    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        int state = writer.state();

        switch (state) {
            case 0:
                if (!writer.writeInt("size", size))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeByteArray("types", types))
                    return false;

                writer.incrementState();

            default:
                int columns = types.length;

                while ((writer.state() - 2) < size * columns) {
                    int i = writer.state() / columns;
                    int j = writer.state() % columns;
                    byte t = types[j];

                    Object[] row = rows.get(i);

                    switch (t) {
                        case 0: // boolean
                            if (!writer.writeBoolean("x" + (writer.state() - 2), (Boolean) row[j]))
                                return false;

                            break;

                        case 1:
                            if (!writer.writeChar("x" + (writer.state() - 2), (Character) row[j]))
                                return false;

                            break;

                        case 2:
                            if (!writer.writeInt("x" + (writer.state() - 2), (Integer) row[j]))
                                return false;

                            break;

                        case 3:
                            if (!writer.writeLong("x" + (writer.state() - 2), (Long) row[j]))
                                return false;

                            break;

                        case 4:
                            if (!writer.writeFloat("x" + (writer.state() - 2), (Float) row[j]))
                                return false;

                            break;

                        case 5:
                            if (!writer.writeDouble("x" + (writer.state() - 2), (Double) row[j]))
                                return false;

                            break;

                        case 6:
                            if (!writer.writeString("x" + (writer.state() - 2), (String) row[j]))
                                return false;

                            break;

                        default:
                            throw new AssertionError("Unexpected type: " + t);
                    }

                    writer.incrementState();
                }
        }

        return true;
    }

    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        return false;
    }

    @Override public short directType() {
        return 0;
    }

    @Override public byte fieldsCount() {
        return -1;
    }

    @Override public void onAckReceived() {
        // No-op
    }
}
