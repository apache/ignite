/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
