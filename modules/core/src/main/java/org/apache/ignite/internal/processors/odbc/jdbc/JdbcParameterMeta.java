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

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC SQL query parameter metadata.
 */
public class JdbcParameterMeta implements JdbcRawBinarylizable {
    /** Null value is allow for the param. */
    private int isNullable;

    /** Signed flag. */
    private boolean signed;

    /** Precision. */
    private int precision;

    /** Scale. */
    private int scale;

    /** SQL type ID. */
    private int type;

    /** SQL type name. */
    private String typeName;

    /** Java type class name. */
    private String typeClass;

    /** Mode. */
    private int mode;


    /**
     * Default constructor is used for binary serialization.
     */
    public JdbcParameterMeta() {
        // No-op.
    }

    /**
     * @param meta Param metadata.
     * @param order Param order.
     * @throws SQLException On errror.
     */
    public JdbcParameterMeta(ParameterMetaData meta, int order) throws SQLException {
        isNullable = meta.isNullable(order);
        signed = meta.isSigned(order);
        precision = meta.getPrecision(order);
        scale = meta.getScale(order);
        type = meta.getParameterType(order);
        typeName = meta.getParameterTypeName(order);
        typeClass = meta.getParameterClassName(order);
        mode = meta.getParameterMode(order);
    }

    /**
     * @return Nullable mode.
     */
    public int isNullable() {
        return isNullable;
    }

    /**
     * @return Signed flag.
     */
    public boolean isSigned() {
        return signed;
    }

    /**
     * @return Precision.
     */
    public int precision() {
        return precision;
    }

    /**
     * @return Scale.
     */
    public int scale() {
        return scale;
    }

    /**
     * @return SQL type.
     */
    public int type() {
        return type;
    }

    /**
     * @return SQL type name.
     */
    public String typeName() {
        return typeName;
    }

    /**
     * @return Java type class name.
     */
    public String typeClass() {
        return typeClass;
    }

    /**
     * @return Mode.
     */
    public int mode() {
        return mode;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer,
        ClientListenerProtocolVersion ver) throws BinaryObjectException {
        writer.writeInt(isNullable);
        writer.writeBoolean(signed);
        writer.writeInt(precision);
        writer.writeInt(scale);
        writer.writeInt(type);
        writer.writeString(typeName);
        writer.writeString(typeClass);
        writer.writeInt(mode);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader,
        ClientListenerProtocolVersion ver) throws BinaryObjectException {
        isNullable = reader.readInt();
        signed = reader.readBoolean();
        precision = reader.readInt();
        scale = reader.readInt();
        type = reader.readInt();
        typeName = reader.readString();
        typeClass = reader.readString();
        mode = reader.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcParameterMeta.class, this);
    }
}
