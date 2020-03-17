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

package org.apache.ignite.internal.processors.query.calcite.exec.exp.type;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

import static org.apache.calcite.rel.type.RelDataType.PRECISION_NOT_SPECIFIED;
import static org.apache.calcite.rel.type.RelDataType.SCALE_NOT_SPECIFIED;

/** */
public class BasicType implements DataType, Externalizable {
    /** */
    private SqlTypeName typeName;

    /** */
    private int precision = PRECISION_NOT_SPECIFIED;

    /** */
    private int scale = SCALE_NOT_SPECIFIED;

    /** */
    private boolean nullable;

    /** */
    public BasicType() {
    }

    /**
     * @param type Source type.
     */
    public BasicType(BasicSqlType type) {
        typeName = type.getSqlTypeName();
        precision = type.getPrecision();
        scale = type.getScale();
        nullable = type.isNullable();
    }

    /** {@inheritDoc} */
    @Override public SqlTypeName typeName() {
        return typeName;
    }

    /** {@inheritDoc} */
    @Override public int precision() {
        return precision;
    }

    /** {@inheritDoc} */
    @Override public int scale() {
        return scale;
    }

    /** */
    @Override public boolean nullable() {
        return nullable;
    }

    /** {@inheritDoc} */
    @Override public RelDataType logicalType(IgniteTypeFactory factory) {
        RelDataType type;

        if (!typeName.allowsPrec())
            type = factory.createSqlType(typeName);
        else if (!typeName.allowsScale())
            type = factory.createSqlType(typeName, precision);
        else
            type = factory.createSqlType(typeName, precision, scale);

        return nullable ? factory.createTypeWithNullability(type, nullable) : type;
    }

    /** {@inheritDoc} */
    @Override public Class<?> javaType(IgniteTypeFactory typeFactory) {
        return (Class<?>) typeFactory.getJavaClass(logicalType(typeFactory));
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(typeName.ordinal());

        if (typeName.allowsScale())
            out.writeInt(scale);

        if (typeName.allowsPrec())
            out.writeInt(precision);

        out.writeBoolean(nullable);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        typeName = SqlTypeName.values()[in.readByte()];

        if (typeName.allowsScale())
            scale = in.readInt();

        if (typeName.allowsPrec())
            precision = in.readInt();

        nullable = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return toString(true);
    }

    /** */
    protected String toString(boolean withNullability) {
        StringBuilder sb = new StringBuilder(typeName.name());

        boolean printPrecision = precision != PRECISION_NOT_SPECIFIED;
        boolean printScale = scale != SCALE_NOT_SPECIFIED;

        if (printPrecision) {
            sb.append('(');
            sb.append(precision);
            if (printScale) {
                sb.append(", ");
                sb.append(scale);
            }
            sb.append(')');
        }

        if (withNullability && !nullable)
            sb.append(" NOT NULL");

        return sb.toString();
    }
}
