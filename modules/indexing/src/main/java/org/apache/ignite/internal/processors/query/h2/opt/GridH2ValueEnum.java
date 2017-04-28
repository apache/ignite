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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
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
    /** Context */
    private final GridKernalContext ctx;

    /** Id of type */
    private final int type;

    /** Integer constant value */
    private final int ordinal;

    /**
     * Enum's constant name.
     * May be null until requested.
     */
    private String name;

    /**
     * Object representation of enum.
     * May be null until requested.
     */
    private CacheObject obj;

    /** */
    public GridH2ValueEnum(GridKernalContext ctx, int type, int val, String name, CacheObject obj) {
        assert ctx != null;
        this.ctx = ctx;
        this.type = type;
        this.ordinal = val;
        this.name = name;
        this.obj = obj;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        return "'" + getString() + "'";
    }

    /** {@inheritDoc} */
    @Override public int getType() {
        return type;
    }

    /** {@inheritDoc} */
    @Override public long getPrecision() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getDisplaySize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public String getString() {
        if (name != null)
            return name;

        BinaryMetadata binMeta = ((CacheObjectBinaryProcessorImpl)ctx.cacheObjects()).metadata0(type);
        if (binMeta == null || !binMeta.isEnum())
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1,
                    new IgniteCheckedException("Cannot get enum string representation. " +
                            "Unknown enum type " + type));

        name = binMeta.getEnumNameByOrdinal(ordinal);
        if (name == null)
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, "Unable to resolve enum constant name [typeId=" +
                type + ", typeName='" + binMeta.typeName() + "', ordinal=" + ordinal + "]");

        return name;
    }

    /** {@inheritDoc} */
    @Override public Object getObject() {
        if (obj != null)
            return obj;

        CacheObjectBinaryProcessorImpl binaryProc = (CacheObjectBinaryProcessorImpl)ctx.cacheObjects();
        BinaryMetadata binMeta = binaryProc.metadata0(type);
        if (binMeta == null || !binMeta.isEnum())
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1,
                    new IgniteCheckedException("Cannot get enum object representation. " +
                            "Unknown enum type " + type));

        obj = new BinaryEnumObjectImpl(binaryProc.binaryContext(), type, null, ordinal);

        return obj;
    }

    /** {@inheritDoc} */
    @Override public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setObject(parameterIndex, getObject(), Types.JAVA_OBJECT);
    }

    /** {@inheritDoc} */
    @Override protected int compareSecure(Value v, CompareMode mode) {
        assert v instanceof GridH2ValueEnum;
        GridH2ValueEnum other = (GridH2ValueEnum)v;

        if (type != other.type)
            return Integer.compare(type, other.type);

        return Integer.compare(ordinal, other.ordinal);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return type ^ ordinal;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object other) {
        if (other == null)
            return false;

        if (!(other instanceof GridH2ValueEnum))
            return false;

        GridH2ValueEnum enumVal = (GridH2ValueEnum)other;
        return type == enumVal.type && enumVal.ordinal == ordinal;
    }

    /** {@inheritDoc} */
    @Override public Value convertTo(int targetType) {
        if (type == targetType)
            return this;

        switch (targetType) {
            case Value.INT:
                return ValueInt.get(ordinal);
            case Value.STRING:
                return ValueString.get(getString());
            case Value.JAVA_OBJECT:
                return ValueJavaObject.getNoCopy(JdbcUtils.serialize(getObject(), null));
            case Value.BYTES:
                return ValueBytes.getNoCopy(JdbcUtils.serialize(getObject(), null));
        }

        throw DbException.get(
                ErrorCode.DATA_CONVERSION_ERROR_1, getString());
    }
}
