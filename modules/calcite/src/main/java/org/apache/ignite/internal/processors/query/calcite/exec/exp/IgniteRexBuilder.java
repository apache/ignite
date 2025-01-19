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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;
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

            if (TypeUtils.hasScale(type) && SqlTypeUtil.isNumeric(type))
                return super.makeLiteral(bd.setScale(type.getScale(), RoundingMode.HALF_UP), type, typeName);
        }

        if(o instanceof DateString)
            o = fixOldDateLiteralValue((DateString)o);
        else if (o instanceof TimestampString)
            o = fixOldTimestampLiteralValue((TimestampString)o);

        return super.makeLiteral(o, type, typeName);
    }

    /** */
    private TimestampString fixOldTimestampLiteralValue(TimestampString tsLiteral) {
        Instant inst = Instant.ofEpochMilli(tsLiteral.getMillisSinceEpoch());

        LocalDateTime locDateTime = LocalDateTime.ofInstant(inst, ZoneOffset.UTC);

        if (beforeGregorian(locDateTime.toLocalDate())) {

        }

        return tsLiteral;
    }

    /** */
    private DateString fixOldDateLiteralValue(DateString dateLiteral) {
        LocalDate d = LocalDate.ofEpochDay(dateLiteral.getDaysSinceEpoch());

        if (beforeGregorian(d)) {
            int oldEpochDays = (int)(java.sql.Date.valueOf(d).getTime() / DateTimeUtils.MILLIS_PER_DAY);

            assert oldEpochDays != dateLiteral.getDaysSinceEpoch();

            d = LocalDate.ofEpochDay(oldEpochDays);

            dateLiteral = new DateString(d.getYear(), d.getMonthValue(), d.getDayOfMonth());
        }

        return dateLiteral;
    }

    /** */
    private boolean beforeGregorian(LocalDate date) {
        if (date.getYear() > 1582)
            return false;

        if (date.getYear() < 1582)
            return true;

        return date.getMonthValue() < 10 || (date.getMonthValue() == 10 && date.getDayOfMonth() < 15);
    }
}
