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

package org.apache.ignite.internal.processors.query.calcite.serialize.type;

import java.util.LinkedHashMap;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

/**
 *
 */
public class StructType implements DataType {
    /** */
    private final LinkedHashMap<String, DataType> fields;

    /**
     * Factory method.
     */
    static StructType fromType(RelDataType type) {
        assert type.isStruct();

        LinkedHashMap<String, DataType> fields = new LinkedHashMap<>();

        for (RelDataTypeField field : type.getFieldList())
            fields.put(field.getName(), DataType.fromType(field.getType()));

        return new StructType(fields);
    }

    /**
     * @param fields Fields.
     */
    private StructType(LinkedHashMap<String, DataType> fields) {
        this.fields = fields;
    }

    /** {@inheritDoc} */
    @Override public RelDataType toRelDataType(RelDataTypeFactory factory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(factory);
        fields.forEach((n,f) -> builder.add(n,f.toRelDataType(factory)));
        return builder.build();
    }
}
