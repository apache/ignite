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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

/**
 * Ignite type system.
 */
public class IgniteTypeSystem extends RelDataTypeSystemImpl implements Serializable {
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
        // Following BasicSqlType precision as the default
        switch (typeName) {
            case CHAR:
            case BINARY:
                return 1;
            case VARCHAR:
            case VARBINARY:
            case DECIMAL:
                return RelDataType.PRECISION_NOT_SPECIFIED;
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                return SqlTypeName.DEFAULT_INTERVAL_START_PRECISION;
            case BOOLEAN:
                return 1;
            case TINYINT:
                return 3;
            case SMALLINT:
                return 5;
            case INTEGER:
                return 10;
            case BIGINT:
                return 19;
            case REAL:
                return 7;
            case FLOAT:
            case DOUBLE:
                return 15;
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case DATE:
                return 0; // SQL99 part 2 section 6.1 syntax rule 30
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // farrago supports only 0 (see
                // SqlTypeName.getDefaultPrecision), but it should be 6
                // (microseconds) per SQL99 part 2 section 6.1 syntax rule 30.
                return 0;
            default:
                return -1;
        }
    }

    /** {@inheritDoc} */
    @Override public RelDataType deriveDecimalPlusType(RelDataTypeFactory typeFactory,
        RelDataType type1, RelDataType type2) {
        if (SqlTypeUtil.isExactNumeric(type1)
            && SqlTypeUtil.isExactNumeric(type2)) {
            if (SqlTypeUtil.isDecimal(type1)
                || SqlTypeUtil.isDecimal(type2)) {
                // Java numeric will always have invalid precision/scale,
                // use its default decimal precision/scale instead.
                type1 = RelDataTypeFactoryImpl.isJavaType(type1)
                    ? typeFactory.decimalOf(type1)
                    : type1;
                type2 = RelDataTypeFactoryImpl.isJavaType(type2)
                    ? typeFactory.decimalOf(type2)
                    : type2;
                int p1 = type1.getPrecision();
                int p2 = type2.getPrecision();
                int s1 = type1.getScale();
                int s2 = type2.getScale();
                int scale = Math.max(s1, s2);
                assert scale <= getMaxNumericScale();
                int precision = Math.max(p1 - s1, p2 - s2) + scale + 1;
                precision =
                    Math.min(
                        precision,
                        getMaxNumericPrecision());

                return typeFactory.createSqlType(
                    SqlTypeName.DECIMAL,
                    precision,
                    scale);
            }
        }
        return null;
    }

}
