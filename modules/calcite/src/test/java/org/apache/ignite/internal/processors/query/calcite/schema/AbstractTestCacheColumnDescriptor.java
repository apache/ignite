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

package org.apache.ignite.internal.processors.query.calcite.schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;

import static org.apache.calcite.rel.type.RelDataType.PRECISION_NOT_SPECIFIED;
import static org.apache.calcite.rel.type.RelDataType.SCALE_NOT_SPECIFIED;

/** Abstract class for tests which allows to avoid redundant boilerplate code. */
public abstract class AbstractTestCacheColumnDescriptor implements CacheColumnDescriptor {
    /** */
    private final int idx;

    /** */
    private final String name;

    /** */
    private final Type type;

    /** */
    private final boolean isKey;

    /** */
    private final boolean isField;

    /** */
    private volatile RelDataType logicalType;

    /** */
    protected AbstractTestCacheColumnDescriptor(int idx, String name, Type type, boolean isKey, boolean isField) {
        this.idx = idx;
        this.name = name;
        this.type = type;
        this.isKey = isKey;
        this.isField = isField;
    }

    /** {@inheritDoc} */
    @Override public boolean field() {
        return isField;
    }

    /** {@inheritDoc} */
    @Override public boolean key() {
        return isKey;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public int fieldIndex() {
        return idx;
    }

    /** {@inheritDoc} */
    @Override public RelDataType logicalType(IgniteTypeFactory f) {
        if (logicalType == null) {
            logicalType = TypeUtils.sqlType(
                f,
                type.cls,
                type.precision != Type.NOT_SPECIFIED ? type.precision : PRECISION_NOT_SPECIFIED,
                type.scale != Type.NOT_SPECIFIED ? type.scale : SCALE_NOT_SPECIFIED,
                type.nullable
            );
        }

        return logicalType;
    }

    /** {@inheritDoc} */
    @Override public Class<?> storageType() {
        return type.cls;
    }

    /** */
    public static class Type {
        /** */
        public static final int NOT_SPECIFIED = -1;

        /** */
        private final Class<?> cls;

        /** */
        private final int precision;

        /** */
        private final int scale;

        /** */
        private final boolean nullable;

        /** */
        private Type(Class<?> cls, int precision, int scale, boolean nullable) {
            this.cls = cls;
            this.precision = precision;
            this.scale = scale;
            this.nullable = nullable;
        }

        /** */
        public static Type nullable(Class<?> cls) {
            return new Type(cls, NOT_SPECIFIED, NOT_SPECIFIED, true);
        }

        /** */
        public static Type notNull(Class<?> cls) {
            return new Type(cls, NOT_SPECIFIED, NOT_SPECIFIED, false);
        }

        /** */
        public static Type of(Class<?> cls, int precision, int scale, boolean nullable) {
            return new Type(cls, precision, scale, nullable);
        }
    }
}
