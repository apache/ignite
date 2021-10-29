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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.ignite.internal.processors.query.calcite.ResultFieldMetadata;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;

/**
 * Results set metadata holder.
 */
public class ResultSetMetadataImpl implements ResultSetMetadataInternal {
    /** Columns origins. */
    private final List<List<String>> origins;

    /** Internal row type. */
    private final RelDataType rowType;

    /** Fields metadata. */
    private volatile List<ResultFieldMetadata> fields;

    public ResultSetMetadataImpl(
        RelDataType rowType,
        List<List<String>> origins
    ) {
        this.rowType = rowType;
        this.origins = origins;
    }

    /** {@inheritDoc} */
    @Override public List<ResultFieldMetadata> fields() {
        if (fields == null) {
            List<ResultFieldMetadata> flds = new ArrayList<>(rowType.getFieldCount());

            for (int i = 0; i < rowType.getFieldCount(); ++i) {
                RelDataTypeField fld = rowType.getFieldList().get(i);

                flds.add(
                    new ResultFieldMetadataImpl(
                        fld.getName(),
                        TypeUtils.nativeType(fld.getType()),
                        fld.getIndex(),
                        fld.getType().isNullable(),
                        origins.get(i)
                    )
                );

                fields = flds;
            }
        }

        return fields;
    }

    /** {@inheritDoc} */
    @Override public RelDataType rowType() {
        return rowType;
    }
}
