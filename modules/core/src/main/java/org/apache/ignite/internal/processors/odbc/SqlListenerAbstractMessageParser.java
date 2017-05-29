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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;

/**
 * ODBC message parser.
 */
public abstract class SqlListenerAbstractMessageParser implements SqlListenerMessageParser {
    /** Initial output stream capacity. */
    protected static final int INIT_CAP = 1024;

    /** Kernal context. */
    protected GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Object reader. */
    private SqlListenerAbstractObjectReader objReader;

    /** Object writer. */
    private SqlListenerAbstractObjectWriter objWriter;

    /**
     * @param ctx Context.
     * @param objReader Object reader.
     * @param objWriter Object writer.
     */
    protected SqlListenerAbstractMessageParser(final GridKernalContext ctx, SqlListenerAbstractObjectReader objReader,
        SqlListenerAbstractObjectWriter objWriter) {
        this.ctx = ctx;

        log = ctx.log(getClass());

        this.objReader = objReader;
        this.objWriter = objWriter;
    }

    /** {@inheritDoc} */
    @Override public SqlListenerRequest decode(byte[] msg) {
        assert msg != null;

        BinaryReaderExImpl reader = createReader(msg);

        return SqlListenerRequest.readRequest(reader, objReader);
    }

    /** {@inheritDoc} */
    @Override public byte[] encode(SqlListenerResponse msg) {
        assert msg != null;

        BinaryWriterExImpl writer = createWriter(INIT_CAP);

        msg.writeBinary(writer, objWriter);

        return writer.array();
    }

    /**
     * @param writer Binary writer.
     * @param objWriter Object writer.
     * @param meta Column metadata.
     */
    public static void writeColumnsMeta(BinaryWriterExImpl writer, SqlListenerAbstractObjectWriter objWriter,
        List<SqlListenerColumnMeta> meta) {
        if (meta == null) {
            writer.writeInt(0);

            return;
        }

        writer.writeInt(meta.size());

        for (SqlListenerColumnMeta columnMeta : meta)
            columnMeta.writeBinary(writer, objWriter);
    }

    /**
     * @param reader Binary reader.
     * @param objReader Object reader.
     * @return List of columns metadata.
     */
    public static List<SqlListenerColumnMeta> readColumnsMeta(BinaryReaderExImpl reader,
        SqlListenerAbstractObjectReader objReader) {
        int metaSize = reader.readInt();

        List<SqlListenerColumnMeta> meta;

        if (metaSize > 0) {
            meta = new ArrayList<>(metaSize);

            for (int i = 0; i < metaSize; ++i) {
                SqlListenerColumnMeta m = new SqlListenerColumnMeta();

                m.readBinary(reader, objReader);

                meta.add(m);
            }
        }
        else
            meta = Collections.emptyList();

        return meta;
    }

    /**
     * @param writer Binari writer.
     * @param objWriter Object writer.
     * @param items Query results items.
     */
    public static void writeItems(BinaryWriterExImpl writer, SqlListenerAbstractObjectWriter objWriter,
        List<List<Object>> items) {
        writer.writeInt(items.size());

        for (List<Object> row : items) {
            if (row != null) {
                writer.writeInt(row.size());

                for (Object obj : row)
                    objWriter.writeObject(writer, obj);
            }
        }
    }

    /**
     * @param reader Binary reader.
     * @param objReader Object reader.
     * @return Query results items.
     */
    public static List<List<Object>> readItems(BinaryReaderExImpl reader, SqlListenerAbstractObjectReader objReader) {
        int rowsSize = reader.readInt();

        if (rowsSize > 0) {
            List<List<Object>> items = new ArrayList<>(rowsSize);

            for (int i = 0; i < rowsSize; ++i) {
                int colsSize = reader.readInt();

                List<Object> col = new ArrayList<>(colsSize);

                for (int colCnt = 0; colCnt < colsSize; ++colCnt)
                    col.add(objReader.readObject(reader));

                items.add(col);
            }

            return items;
        } else
            return Collections.emptyList();
    }

    /**
     * Create reader.
     *
     * @param msg Input message.
     * @return Reader.
     */
    protected abstract BinaryReaderExImpl createReader(byte[] msg);

    /**
     * Create writer.
     *
     * @param cap Initial capacity.
     * @return Binary writer instance.
     */
    protected abstract BinaryWriterExImpl createWriter(int cap);
}
