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

package org.apache.ignite.internal.processors.odbc.jdbc;

import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
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
    @Override public void writeBinary(BinaryWriterExImpl writer,
        ClientListenerProtocolVersion ver) {
        super.writeBinary(writer, ver);

        writer.writeInt(precision);
        writer.writeInt(scale);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader,
        ClientListenerProtocolVersion ver) {
        super.readBinary(reader, ver);

        precision = reader.readInt();
        scale = reader.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcColumnMetaV4.class, this);
    }
}
