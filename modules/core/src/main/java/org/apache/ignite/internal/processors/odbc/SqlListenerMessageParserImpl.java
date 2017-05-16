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

package org.apache.ignite.internal.processors.odbc;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.jetbrains.annotations.Nullable;

/**
 * ODBC message parser.
 */
public abstract class SqlListenerMessageParserImpl implements SqlListenerMessageParser {
    /** Initial output stream capacity. */
    protected static final int INIT_CAP = 1024;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param ctx Context.
     */
    protected SqlListenerMessageParserImpl(final GridKernalContext ctx) {
        log = ctx.log(getClass());
    }

    /** {@inheritDoc} */
    @Override public SqlListenerRequest decode(byte[] msg) {
        assert msg != null;

        BinaryInputStream stream = new BinaryHeapInputStream(msg);

        BinaryReaderExImpl reader = createBinaryReader(stream);

        byte cmd = reader.readByte();

        SqlListenerRequest res;

        switch (cmd) {
            case SqlListenerRequest.QRY_EXEC: {
                String cache = reader.readString();
                String sql = reader.readString();
                int argsNum = reader.readInt();

                Object[] params = new Object[argsNum];

                for (int i = 0; i < argsNum; ++i)
                    params[i] = readObject(reader);

                res = new SqlListenerQueryExecuteRequest(cache, sql, params);

                break;
            }

            case SqlListenerRequest.QRY_FETCH: {
                long queryId = reader.readLong();
                int pageSize = reader.readInt();

                res = new SqlListenerQueryFetchRequest(queryId, pageSize);

                break;
            }

            case SqlListenerRequest.QRY_CLOSE: {
                long queryId = reader.readLong();

                res = new SqlListenerQueryCloseRequest(queryId);

                break;
            }

            case SqlListenerRequest.META_COLS: {
                String cache = reader.readString();
                String table = reader.readString();
                String column = reader.readString();

                res = new OdbcQueryGetColumnsMetaRequest(cache, table, column);

                break;
            }

            case SqlListenerRequest.META_TBLS: {
                String catalog = reader.readString();
                String schema = reader.readString();
                String table = reader.readString();
                String tableType = reader.readString();

                res = new OdbcQueryGetTablesMetaRequest(catalog, schema, table, tableType);

                break;
            }

            case SqlListenerRequest.META_PARAMS: {
                String cacheName = reader.readString();
                String sqlQuery = reader.readString();

                res = new OdbcQueryGetParamsMetaRequest(cacheName, sqlQuery);

                break;
            }

            default:
                throw new IgniteException("Unknown ODBC command: [cmd=" + cmd + ']');
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public byte[] encode(SqlListenerResponse msg) {
        assert msg != null;

        // Creating new binary writer
        BinaryWriterExImpl writer = createBinaryWriter(INIT_CAP);

        // Writing status.
        writer.writeByte((byte) msg.status());

        if (msg.status() != SqlListenerResponse.STATUS_SUCCESS) {
            writer.writeString(msg.error());

            return writer.array();
        }

        Object res0 = msg.response();

        if (res0 == null)
            return writer.array();
        else if (res0 instanceof SqlListenerQueryExecuteResult) {
            SqlListenerQueryExecuteResult res = (SqlListenerQueryExecuteResult) res0;

            if (log.isDebugEnabled())
                log.debug("Resulting query ID: " + res.getQueryId());

            writer.writeLong(res.getQueryId());

            Collection<SqlListenerColumnMeta> metas = res.getColumnsMetadata();

            assert metas != null;

            writer.writeInt(metas.size());

            for (SqlListenerColumnMeta meta : metas)
                meta.write(writer);
        }
        else if (res0 instanceof SqlListenerQueryFetchResult) {
            SqlListenerQueryFetchResult res = (SqlListenerQueryFetchResult) res0;

            if (log.isDebugEnabled())
                log.debug("Resulting query ID: " + res.queryId());

            writer.writeLong(res.queryId());

            Collection<?> items0 = res.items();

            assert items0 != null;

            writer.writeBoolean(res.last());

            writer.writeInt(items0.size());

            for (Object row0 : items0) {
                if (row0 != null) {
                    Collection<?> row = (Collection<?>)row0;

                    writer.writeInt(row.size());

                    for (Object obj : row)
                        writeObject(writer, obj);
                }
            }
        }
        else if (res0 instanceof SqlListenerQueryCloseResult) {
            SqlListenerQueryCloseResult res = (SqlListenerQueryCloseResult) res0;

            if (log.isDebugEnabled())
                log.debug("Resulting query ID: " + res.getQueryId());

            writer.writeLong(res.getQueryId());
        }
        else if (res0 instanceof OdbcQueryGetColumnsMetaResult) {
            OdbcQueryGetColumnsMetaResult res = (OdbcQueryGetColumnsMetaResult) res0;

            Collection<SqlListenerColumnMeta> columnsMeta = res.meta();

            assert columnsMeta != null;

            writer.writeInt(columnsMeta.size());

            for (SqlListenerColumnMeta columnMeta : columnsMeta)
                columnMeta.write(writer);
        }
        else if (res0 instanceof OdbcQueryGetTablesMetaResult) {
            OdbcQueryGetTablesMetaResult res = (OdbcQueryGetTablesMetaResult) res0;

            Collection<OdbcTableMeta> tablesMeta = res.meta();

            assert tablesMeta != null;

            writer.writeInt(tablesMeta.size());

            for (OdbcTableMeta tableMeta : tablesMeta)
                tableMeta.writeBinary(writer);
        }
        else if (res0 instanceof OdbcQueryGetParamsMetaResult) {
            OdbcQueryGetParamsMetaResult res = (OdbcQueryGetParamsMetaResult) res0;

            byte[] typeIds = res.typeIds();

            writeObject(writer, typeIds);
        }
        else
            assert false : "Should not reach here.";

        return writer.array();
    }

    /**
     * @param reader Reader.
     * @return Object.
     * @throws BinaryObjectException On error.
     */
    private Object readObject(BinaryReaderExImpl reader) throws BinaryObjectException {
        byte type = reader.readByte();

        switch (type) {
            case GridBinaryMarshaller.NULL:
                return null;

            case GridBinaryMarshaller.BOOLEAN:
                return reader.readBoolean();

            case GridBinaryMarshaller.BYTE:
                return reader.readByte();

            case GridBinaryMarshaller.CHAR:
                return reader.readChar();

            case GridBinaryMarshaller.SHORT:
                return reader.readShort();

            case GridBinaryMarshaller.INT:
                return reader.readInt();

            case GridBinaryMarshaller.LONG:
                return reader.readLong();

            case GridBinaryMarshaller.FLOAT:
                return reader.readFloat();

            case GridBinaryMarshaller.DOUBLE:
                return reader.readDouble();

            case GridBinaryMarshaller.STRING:
                return BinaryUtils.doReadString(reader.in());

            case GridBinaryMarshaller.DECIMAL:
                return BinaryUtils.doReadDecimal(reader.in());

            case GridBinaryMarshaller.UUID:
                return BinaryUtils.doReadUuid(reader.in());

            case GridBinaryMarshaller.TIME:
                return BinaryUtils.doReadTime(reader.in());

            case GridBinaryMarshaller.TIMESTAMP:
                return BinaryUtils.doReadTimestamp(reader.in());

            case GridBinaryMarshaller.DATE:
                return BinaryUtils.doReadSqlDate(reader.in());

            case GridBinaryMarshaller.BOOLEAN_ARR:
                return BinaryUtils.doReadBooleanArray(reader.in());

            case GridBinaryMarshaller.BYTE_ARR:
                return BinaryUtils.doReadByteArray(reader.in());

            case GridBinaryMarshaller.CHAR_ARR:
                return BinaryUtils.doReadCharArray(reader.in());

            case GridBinaryMarshaller.SHORT_ARR:
                return BinaryUtils.doReadShortArray(reader.in());

            case GridBinaryMarshaller.INT_ARR:
                return BinaryUtils.doReadIntArray(reader.in());

            case GridBinaryMarshaller.FLOAT_ARR:
                return BinaryUtils.doReadFloatArray(reader.in());

            case GridBinaryMarshaller.DOUBLE_ARR:
                return BinaryUtils.doReadDoubleArray(reader.in());

            case GridBinaryMarshaller.STRING_ARR:
                return BinaryUtils.doReadStringArray(reader.in());

            case GridBinaryMarshaller.DECIMAL_ARR:
                return BinaryUtils.doReadDecimalArray(reader.in());

            case GridBinaryMarshaller.UUID_ARR:
                return BinaryUtils.doReadUuidArray(reader.in());

            case GridBinaryMarshaller.TIME_ARR:
                return BinaryUtils.doReadTimeArray(reader.in());

            case GridBinaryMarshaller.TIMESTAMP_ARR:
                return BinaryUtils.doReadTimestampArray(reader.in());

            case GridBinaryMarshaller.DATE_ARR:
                return BinaryUtils.doReadSqlDateArray(reader.in());

            case SqlListenerMessageParser.JDK_MARSH:
                return readJdkMarshalledObject(reader);

            default:
                reader.in().position(reader.in().position() - 1);

                return reader.readObjectDetached();
        }
    }

    /**
     * @param writer Writer.
     * @param obj Object to write,
     * @throws BinaryObjectException On error.
     */
    public void writeObject(BinaryWriterExImpl writer, @Nullable Object obj) throws BinaryObjectException {
        if (obj == null) {
            writer.writeByte(GridBinaryMarshaller.NULL);

            return;
        }

        Class<?> cls = obj.getClass();

        if (cls == Boolean.class)
            writer.writeBooleanFieldPrimitive((Boolean)obj);
        else if (cls == Byte.class)
            writer.writeByteFieldPrimitive((Byte)obj);
        else if (cls == Character.class)
            writer.writeCharFieldPrimitive((Character)obj);
        else if (cls == Short.class)
            writer.writeShortFieldPrimitive((Short)obj);
        else if (cls == Integer.class)
            writer.writeIntFieldPrimitive((Integer)obj);
        else if (cls == Long.class)
            writer.writeLongFieldPrimitive((Long)obj);
        else if (cls == Float.class)
            writer.writeFloatFieldPrimitive((Float)obj);
        else if (cls == Double.class)
            writer.writeDoubleFieldPrimitive((Double)obj);
        else if (cls == String.class)
            writer.doWriteString((String)obj);
        else if (cls == BigDecimal.class)
            writer.doWriteDecimal((BigDecimal)obj);
        else if (cls == UUID.class)
            writer.writeUuid((UUID)obj);
        else if (cls == Time.class)
            writer.writeTime((Time)obj);
        else if (cls == Timestamp.class)
            writer.writeTimestamp((Timestamp)obj);
        else if (cls == Date.class)
            writer.writeDate((Date)obj);
        else if (cls == boolean[].class)
            writer.writeBooleanArray((boolean[])obj);
        else if (cls == byte[].class)
            writer.writeByteArray((byte[])obj);
        else if (cls == char[].class)
            writer.writeCharArray((char[])obj);
        else if (cls == short[].class)
            writer.writeShortArray((short[])obj);
        else if (cls == int[].class)
            writer.writeIntArray((int[])obj);
        else if (cls == float[].class)
            writer.writeFloatArray((float[])obj);
        else if (cls == double[].class)
            writer.writeDoubleArray((double[])obj);
        else if (cls == String[].class)
            writer.writeStringArray((String[])obj);
        else if (cls == BigDecimal[].class)
            writer.writeDecimalArray((BigDecimal[])obj);
        else if (cls == UUID[].class)
            writer.writeUuidArray((UUID[])obj);
        else if (cls == Time[].class)
            writer.writeTimeArray((Time[])obj);
        else if (cls == Timestamp[].class)
            writer.writeTimestampArray((Timestamp[])obj);
        else if (cls == Date[].class)
            writer.writeDateArray((Date[])obj);
        else
            writeNotEmbeddedObject(writer, obj);
    }

    /**
     * @param writer Writer.
     * @param obj Object to marshal with marshaller and write to binary stream.
     * @throws BinaryObjectException On error.
     */
    protected abstract void writeNotEmbeddedObject(BinaryWriterExImpl writer, Object obj) throws BinaryObjectException;

    /**
     * @param reader Reader.
     * @return An object is unmarshaled by marshaller.
     * @throws BinaryObjectException On error.
     */
    protected abstract Object readJdkMarshalledObject(BinaryReaderExImpl reader) throws BinaryObjectException;

    /**
     * @param cap Initial capacity.
     * @return Binary writer instance.
     */
    protected abstract BinaryWriterExImpl createBinaryWriter(int cap);

    /**
     * @param in Binary input stream.
     * @return Binary writer instance.
     */
    protected abstract BinaryReaderExImpl createBinaryReader(BinaryInputStream in);
}
