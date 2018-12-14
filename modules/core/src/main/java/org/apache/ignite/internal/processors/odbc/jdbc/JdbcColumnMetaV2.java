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
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC column metadata.
 */
public class JdbcColumnMetaV2 extends JdbcColumnMeta{
    /** Allow nulls . */
    private boolean nullable;

    /**
     * Default constructor is used for serialization.
     */
    JdbcColumnMetaV2() {
        // No-op.
    }

    /**
     * @param schemaName Schema.
     * @param tblName Table.
     * @param colName Column.
     * @param cls Type.
     * @param nullable Allow nulls.
     */
    public JdbcColumnMetaV2(String schemaName, String tblName, String colName, Class<?> cls, boolean nullable) {
        super(schemaName, tblName, colName, cls);

        this.nullable = nullable;
    }

    /** {@inheritDoc} */
    @Override public boolean isNullable() {
        return nullable;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer,
        ClientListenerProtocolVersion ver) {
        super.writeBinary(writer, ver);

        writer.writeBoolean(nullable);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader,
        ClientListenerProtocolVersion ver) {
        super.readBinary(reader, ver);

        nullable = reader.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcColumnMetaV2.class, this);
    }
}
