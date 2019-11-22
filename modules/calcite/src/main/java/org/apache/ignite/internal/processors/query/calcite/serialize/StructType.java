/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.serialize;

import java.util.LinkedHashMap;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

/**
 *
 */
public class StructType implements ExpDataType {
    private final LinkedHashMap<String, ExpDataType> fields;

    static StructType fromType(RelDataType type) {
        assert type.isStruct();

        LinkedHashMap<String, ExpDataType> fields = new LinkedHashMap<>();

        for (RelDataTypeField field : type.getFieldList()) {
            fields.put(field.getName(), ExpDataType.fromType(field.getType()));
        }

        return new StructType(fields);
    }

    private StructType(LinkedHashMap<String, ExpDataType> fields) {
        this.fields = fields;
    }

    @Override public RelDataType toRelDataType(RelDataTypeFactory factory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(factory);
        fields.forEach((n,f) -> builder.add(n,f.toRelDataType(factory)));
        return builder.build();
    }
}
