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

import java.util.function.Supplier;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.schema.NativeType;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.calcite.util.Commons.nativeTypeToClass;

/** */
public class ColumnDescriptorImpl implements ColumnDescriptor {
    private final boolean key;

    private final String name;

    /** */
    private final @Nullable Supplier<Object> dfltVal;

    /** */
    private final int fieldIdx;

    /** */
    private final NativeType storageType;

    public ColumnDescriptorImpl(
        String name,
        boolean key,
        int fieldIdx,
        NativeType storageType,
        @Nullable Supplier<Object> dfltVal
    ) {
        this.key = key;
        this.name = name;
        this.dfltVal = dfltVal;
        this.fieldIdx = fieldIdx;
        this.storageType = storageType;
    }

    /** {@inheritDoc} */
    @Override public boolean key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public boolean hasDefaultValue() {
        return dfltVal != null;
    }

    /** {@inheritDoc} */
    @Override public Object defaultValue() {
        return dfltVal != null ? dfltVal.get() : null;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public int fieldIndex() {
        return fieldIdx;
    }

    /** {@inheritDoc} */
    @Override public RelDataType logicalType(IgniteTypeFactory f) {
        return f.createJavaType(nativeTypeToClass(storageType));
    }

    /** {@inheritDoc} */
    @Override public NativeType storageType() {
        return storageType;
    }
}
