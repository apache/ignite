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
 * JDBC column metadata V3.
 */
public class JdbcColumnMetaV3 extends JdbcColumnMetaV2 {
    /** Default value. */
    private String dfltValue;

    /**
     * Default constructor is used for serialization.
     */
    JdbcColumnMetaV3() {
        // No-op.
    }

    /**
     * @param schemaName Schema.
     * @param tblName Table.
     * @param colName Column.
     * @param cls Type.
     * @param nullable Allow nulls.
     * @param dfltVal Default value.
     */
    public JdbcColumnMetaV3(String schemaName, String tblName, String colName, Class<?> cls, boolean nullable,
        Object dfltVal) {
        super(schemaName, tblName, colName, cls, nullable);

        if (dfltVal == null)
            dfltValue = null;
        else {
            if (dfltVal instanceof String)
                dfltValue = "'" + String.valueOf(dfltVal) + "'";
            else
                dfltValue = String.valueOf(dfltVal);
        }
    }

    /** {@inheritDoc} */
    @Override public String defaultValue() {
        return dfltValue;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(
        BinaryWriterExImpl writer,
        JdbcProtocolContext protoCtx
    ) {
        super.writeBinary(writer, protoCtx);

        writer.writeString(dfltValue);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(
        BinaryReaderExImpl reader,
        JdbcProtocolContext protoCtx
    ) {
        super.readBinary(reader, protoCtx);

        dfltValue = reader.readString();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcColumnMetaV3.class, this);
    }
}
