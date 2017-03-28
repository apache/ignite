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

package org.apache.ignite.internal.processors.query.h2.opt;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.JdbcUtils;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueInt;
import org.h2.value.ValueBytes;
import org.h2.value.ValueString;
import org.h2.value.ValueJavaObject;


import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

/**
 * H2 Value over enum
 */
public class GridH2ValueEnum extends Value {

    /** Id of type */
    private final int type;

    /** Enum class */
    private final Class<?> cls;

    /** Integer constant value */
    private final int ordinal;

    /** Enum's constant name */
    private final String name;

    /** */
    public GridH2ValueEnum(int type, Class<?> cls, int val, String name) {
        this.type = type;
        this.cls = cls;
        this.ordinal = val;
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override
    public String getSQL() {
        return "'" + name + "'";
    }

    /** {@inheritDoc} */
    @Override
    public int getType() {
        return type;
    }

    /** {@inheritDoc} */
    @Override
    public long getPrecision() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public int getDisplaySize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public String getString() {
        return name;
    }

    /** {@inheritDoc} */
    @Override
    public Object getObject() {
        return cls.getEnumConstants()[ordinal];
    }

    /** {@inheritDoc} */
    @Override
    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        Object obj = JdbcUtils.deserialize(getBytesNoCopy(), getDataHandler());
        prep.setObject(parameterIndex, obj, Types.JAVA_OBJECT);
    }

    /** {@inheritDoc} */
    @Override
    protected int compareSecure(Value v, CompareMode mode) {
        assert v instanceof GridH2ValueEnum;
        GridH2ValueEnum other = (GridH2ValueEnum)v;
        return Integer.compare(ordinal, other.ordinal);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return ordinal;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object other) {
        if (other == null)
            return false;

        if (!(other instanceof GridH2ValueEnum))
            return false;

        GridH2ValueEnum enumVal = (GridH2ValueEnum)other;
        return type == enumVal.type && enumVal.ordinal == ordinal;
    }

    /** {@inheritDoc} */
    @Override
    public Value convertTo(int targetType) {
        if (type == targetType)
            return this;

        switch (targetType) {
            case Value.INT:
                return ValueInt.get(ordinal);
            case Value.STRING:
                return ValueString.get(name);
            case Value.JAVA_OBJECT:
                return ValueJavaObject.getNoCopy(JdbcUtils.serialize(ordinal, null));
            case Value.BYTES:
                return ValueBytes.getNoCopy(JdbcUtils.serialize(ordinal, null));
        }

        throw DbException.get(
                ErrorCode.DATA_CONVERSION_ERROR_1, getString());
    }

    /** Get class */
    public Class<?> getEnumClass() {
        return cls;
    }
}