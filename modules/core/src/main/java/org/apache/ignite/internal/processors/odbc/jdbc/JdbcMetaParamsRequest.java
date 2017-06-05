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

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC SQL query parameters metadata request.
 */
public class JdbcMetaParamsRequest extends JdbcRequest {
    /** Cache. */
    private String schema;

    /** Query. */
    private String sql;

    /**
     * Default constructor is used for deserialization.
     */
    JdbcMetaParamsRequest() {
        super(META_PARAMS);
    }

    /**
     * @param schema Schema name.
     * @param sql SQL Query.
     */
    public JdbcMetaParamsRequest(String schema, String sql) {
        super(META_PARAMS);

        this.schema = schema;
        this.sql = sql;
    }

    /**
     * @return SQL Query.
     */
    public String sql() {
        return sql;
    }

    /**
     * @return Cache name.
     */
    public String schema() {
        return schema;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException {
        super.writeBinary(writer);

        writer.writeString(schema);
        writer.writeString(sql);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException {
        super.readBinary(reader);

        schema = reader.readString();
        sql = reader.readString();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcMetaParamsRequest.class, this);
    }
}
