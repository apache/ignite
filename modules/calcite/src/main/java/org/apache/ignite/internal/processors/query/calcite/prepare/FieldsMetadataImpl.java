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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.lang.reflect.Type;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.util.typedef.F;

/** */
public class FieldsMetadataImpl implements FieldsMetadata {
    /** */
    private final RelDataType rowType;

    /** */
    private final RelDataType sqlRowType;

    /** */
    private final List<List<String>> origins;

    /** */
    private final List<String> aliases;

    /** */
    public FieldsMetadataImpl(RelDataType rowType, List<List<String>> origins, List<String> aliases) {
        this(rowType, rowType, origins, aliases);
    }

    /** */
    public FieldsMetadataImpl(
        RelDataType sqlRowType,
        RelDataType rowType,
        List<List<String>> origins,
        List<String> aliases
    ) {
        this.rowType = rowType;
        this.sqlRowType = sqlRowType;
        this.origins = origins;
        this.aliases = aliases;
    }


    /** {@inheritDoc} */
    @Override public RelDataType rowType() {
        return rowType;
    }

    /** {@inheritDoc} */
    @Override public List<GridQueryFieldMetadata> queryFieldsMetadata(IgniteTypeFactory typeFactory) {
        List<RelDataTypeField> fields = sqlRowType.getFieldList();

        assert F.isEmpty(origins) || fields.size() == origins.size();

        ImmutableList.Builder<GridQueryFieldMetadata> b = ImmutableList.builder();

        for (int i = 0; i < fields.size(); i++) {
            List<String> origin = !F.isEmpty(origins) ? origins.get(i) : null;
            String alias = aliases != null && aliases.size() > i ? aliases.get(i) : null;
            RelDataTypeField field = fields.get(i);
            RelDataType fieldType = field.getType();
            Type fieldCls = typeFactory.getResultClass(fieldType);

            b.add(new CalciteQueryFieldMetadata(
                F.isEmpty(origin) ? null : origin.get(0),
                F.isEmpty(origin) ? null : origin.get(1),
                alias != null ? alias : F.isEmpty(origin) ? field.getName() : origin.get(2),
                fieldCls == null ? Void.class.getName() : fieldCls.getTypeName(),
                fieldType.getPrecision(),
                fieldType.getScale(),
                fieldType.isNullable()
            ));
        }

        return b.build();
    }
}
