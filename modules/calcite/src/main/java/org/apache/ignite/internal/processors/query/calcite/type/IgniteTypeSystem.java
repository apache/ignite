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

package org.apache.ignite.internal.processors.query.calcite.type;

import java.io.Serializable;
import java.math.BigDecimal;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Ignite type system.
 */
public class IgniteTypeSystem extends RelDataTypeSystemImpl implements Serializable {
    /** Singleton instance. */
    public static final RelDataTypeSystem INSTANCE = new IgniteTypeSystem();

    /** {@inheritDoc} */
    @Override public int getMaxNumericScale() {
        return Short.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override public int getMaxNumericPrecision() {
        return Short.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override public int getDefaultPrecision(SqlTypeName typeName) {
        // Timestamps internally stored as millis, precision more than 3 is redundant. At the same time,
        // default Calcite precision 0 causes truncation when converting to TIMESTAMP without specifying precision.
        if (typeName == SqlTypeName.TIMESTAMP || typeName == SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
            return 3;

        return super.getDefaultPrecision(typeName);
    }

    /** {@inheritDoc} */
    @Override public RelDataType deriveSumType(RelDataTypeFactory typeFactory, RelDataType argumentType) {
        RelDataType sumType;
        if (argumentType instanceof BasicSqlType) {
            switch (argumentType.getSqlTypeName()) {
                case INTEGER:
                case TINYINT:
                case SMALLINT:
                    sumType = typeFactory.createSqlType(SqlTypeName.BIGINT);

                    break;

                case BIGINT:
                case DECIMAL:
                    sumType = typeFactory.createSqlType(SqlTypeName.DECIMAL);

                    break;

                case REAL:
                case FLOAT:
                case DOUBLE:
                    sumType = typeFactory.createSqlType(SqlTypeName.DOUBLE);

                    break;

                default:
                    return super.deriveSumType(typeFactory, argumentType);
            }
        }
        else {
            switch (argumentType.getSqlTypeName()) {
                case INTEGER:
                case TINYINT:
                case SMALLINT:
                    sumType = typeFactory.createJavaType(Long.class);

                    break;

                case BIGINT:
                case DECIMAL:
                    sumType = typeFactory.createJavaType(BigDecimal.class);

                    break;

                case REAL:
                case FLOAT:
                case DOUBLE:
                    sumType = typeFactory.createJavaType(Double.class);

                    break;

                default:
                    return super.deriveSumType(typeFactory, argumentType);
            }
        }

        return typeFactory.createTypeWithNullability(sumType, argumentType.isNullable());
    }

    /** {@inheritDoc} */
    @Override public boolean shouldConvertRaggedUnionTypesToVarying() {
        return true;
    }
}
