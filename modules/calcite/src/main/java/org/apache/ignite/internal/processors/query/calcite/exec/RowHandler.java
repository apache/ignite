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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.lang.reflect.Type;
import java.util.List;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

/**
 * Universal accessor and mutator for rows. It also has factory methods.
 */
public interface RowHandler<Row> {
    /** */
    Object get(int field, Row row);

    /** */
    void set(int field, Row row, Object val);

    /** */
    Row concat(Row left, Row right);

    /** */
    int columnCount(Row row);

    /** */
    default RowFactory<Row> factory(IgniteTypeFactory typeFactory, RelDataType rowType) {
        if (rowType.isStruct())
            return factory(typeFactory, RelOptUtil.getFieldTypeList(rowType));

        return factory(typeFactory.getJavaClass(rowType));
    }

    /** */
    default RowFactory<Row> factory(IgniteTypeFactory typeFactory, List<RelDataType> fieldTypes) {
        Type[] types = new Type[fieldTypes.size()];
        for (int i = 0; i < fieldTypes.size(); i++)
            types[i] = typeFactory.getJavaClass(fieldTypes.get(i));

        return factory(types);
    }

    RowFactory<Row> factory(Type... types);

    @SuppressWarnings("PublicInnerClass")
    interface RowFactory<Row> {
        /** */
        Row create();

        /** */
        Row create(Object... fields);
    }
}
