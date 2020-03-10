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

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.text.StrBuilder;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

import static org.apache.calcite.rel.type.RelDataType.PRECISION_NOT_SPECIFIED;
import static org.apache.calcite.rel.type.RelDataType.SCALE_NOT_SPECIFIED;

/** */
public class StructType implements DataType {
    /** */
    private final LinkedHashMap<String, DataType> fields;

    /**
     * @param fields Fields.
     */
    public StructType(LinkedHashMap<String, DataType> fields) {
        this.fields = fields;
    }

    public Map<String, DataType> fields() {
        return fields;
    }

    /** {@inheritDoc} */
    @Override public SqlTypeName typeName() {
        return SqlTypeName.ROW;
    }

    /** {@inheritDoc} */
    @Override public int precision() {
        return PRECISION_NOT_SPECIFIED;
    }

    /** {@inheritDoc} */
    @Override public int scale() {
        return SCALE_NOT_SPECIFIED;
    }

    /** {@inheritDoc} */
    @Override public boolean nullable() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public RelDataType logicalType(IgniteTypeFactory factory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(factory);
        fields.forEach((n,f) -> builder.add(n,f.logicalType(factory)));
        return builder.build();
    }

    /** {@inheritDoc} */
    @Override public Class<?> javaType(IgniteTypeFactory typeFactory) {
        return Object[].class; // TODO currently a row is mapped to an object array, will be changed in future.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StrBuilder sb = new StrBuilder("Row(");

        boolean first = true;

        for (Map.Entry<String, DataType> field : fields.entrySet()) {
            if(first)
                first = false;
            else
                sb.append(", ");

            sb.append("\"")
                .append(field.getKey())
                .append("\":")
                .append(field.getValue());
        }

        sb.append(")");

        return sb.toString();
    }
}
