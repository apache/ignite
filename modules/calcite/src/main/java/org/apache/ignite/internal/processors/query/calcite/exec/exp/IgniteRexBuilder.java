/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.exec.exp;

import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

/** */
public class IgniteRexBuilder extends RexBuilder {
    /** */
    public IgniteRexBuilder(RelDataTypeFactory typeFactory) {
        super(typeFactory);
    }

    /** {@inheritDoc} */
    @Override protected RexLiteral makeLiteral(@Nullable Comparable o, RelDataType type, SqlTypeName typeName) {
        if (o != null && typeName == SqlTypeName.DECIMAL) {
            BigDecimal bd = (BigDecimal)o;

            if (type.getSqlTypeName() == SqlTypeName.BIGINT) {
                try {
                    bd.longValueExact();
                }
                catch (ArithmeticException e) {
                    throw new IgniteSQLException(SqlTypeName.BIGINT.getName() + " overflow", e);
                }
            }

            if (type instanceof IntervalSqlType) {
                // TODO Workaround for https://issues.apache.org/jira/browse/CALCITE-6714
                bd = bd.multiply(((IntervalSqlType)type).getIntervalQualifier().getUnit().multiplier);

                return super.makeLiteral(bd, type, type.getSqlTypeName());
            }

            if (SqlTypeUtil.isNumeric(type)) {
                boolean dfltDecimal = SqlTypeName.DECIMAL == type.getSqlTypeName() && bd.scale() > 0
                    && typeFactory.getTypeSystem().getDefaultScale(SqlTypeName.DECIMAL) == type.getScale()
                    && typeFactory.getTypeSystem().getDefaultPrecision(SqlTypeName.DECIMAL) == type.getPrecision();

                // Keeps scaled values for literals like DECIMAL (converted to DECIMAL(32676, 0)) like in Postgres,
                // keeping actual scale. Example: 2::FLOAT is "2", not "2.0".
                if (dfltDecimal) {
                    int precision = Math.max(bd.precision(), bd.scale());

                    type = typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, bd.scale());
                }

                if (TypeUtils.hasScale(type) || dfltDecimal)
                    return super.makeLiteral(bd.setScale(type.getScale(), RoundingMode.HALF_UP), type, typeName);
            }

            return super.makeLiteral(bd, type, typeName);
        }

        return super.makeLiteral(o, type, typeName);
    }
}
