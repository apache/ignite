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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;

/** Ignite type coercion. */
public class IgniteTypeCoercion extends TypeCoercionImpl {
    /** */
    public IgniteTypeCoercion(RelDataTypeFactory typeFactory, SqlValidator validator) {
        super(typeFactory, validator);
    }

    /** {@inheritDoc} */
    @Override protected boolean needToCast(SqlValidatorScope scope, SqlNode node, RelDataType toType) {
        if (SqlTypeUtil.isIntType(toType)) {
            RelDataType fromType = validator.deriveType(scope, node);

            if (fromType == null)
                return false;

            if (SqlTypeUtil.isIntType(fromType) && fromType.getSqlTypeName() != toType.getSqlTypeName())
                return true;
        }

        return super.needToCast(scope, node, toType);
    }
}
