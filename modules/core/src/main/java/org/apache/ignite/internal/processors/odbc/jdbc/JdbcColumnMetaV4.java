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

package org.apache.ignite.internal.processors.odbc.jdbc;

import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC column metadata V4.
 */
public class JdbcColumnMetaV4 extends JdbcColumnMetaV3 {
    /** Decimal field precision. */
    private int precision;

    /** Decimal field scale. */
    private int scale;

    /**
     * Default constructor is used for serialization.
     */
    JdbcColumnMetaV4() {
        // No-op.
    }

    /**
     * @param schemaName Schema.
     * @param tblName Table.
     * @param colName Column.
     * @param cls Type.
     * @param nullable Allow nulls.
     * @param dfltVal Default value.
     * @param precision Decimal column precision.
     * @param scale Decimal column scale.
     */
    public JdbcColumnMetaV4(String schemaName, String tblName, String colName, Class<?> cls, boolean nullable,
        Object dfltVal, int precision, int scale) {
        super(schemaName, tblName, colName, cls, nullable, dfltVal);

        this.precision = precision;

        this.scale = scale;
    }

    /** {@inheritDoc} */
    @Override public int precision() {
        return precision;
    }

    /** {@inheritDoc} */
    @Override public int scale() {
        return scale;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(
        BinaryWriterExImpl writer,
        JdbcProtocolContext protoCtx
    ) {
        super.writeBinary(writer, protoCtx);

        writer.writeInt(precision);
        writer.writeInt(scale);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(
        BinaryReaderExImpl reader,
        JdbcProtocolContext protoCtx
    ) {
        super.readBinary(reader, protoCtx);

        precision = reader.readInt();
        scale = reader.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcColumnMetaV4.class, this);
    }
}
