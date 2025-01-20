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
import java.time.LocalTime;
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

        // Adjust temporal literals so that dates before 15.10.1582 will match internal long representation.
        // RexLiteral#RexLiteral(Class) uses DateTimeUtils#ymdToUnixDate(int year, int month, int day) which directly
        // calculates epoch days. We use internal new Date(long) and new Timestampt(long) in TypeUtils#fromInternal(...) and
        // and Date#getTime() in TypeUtils#toInternal(...). Classic temporal types recalculate the long value before
        // Gregorian calendar. DateTimeUtils does not. This causes about +10 days from old dates literals with.
        if(o instanceof DateString)
            o = fixOldDateLiteralValue((DateString)o);
        else if (o instanceof TimestampString)
            o = fixOldTimestampLiteralValue((TimestampString)o);

        return super.makeLiteral(o, type, typeName);
    }

    /** */
    private DateString fixOldDateLiteralValue(DateString dateLiteral) {
        LocalDate locDate = LocalDate.ofEpochDay(dateLiteral.getDaysSinceEpoch());

        if (beforeGregorian(locDate)) {
            int recalculatedEpochDays = (int)(java.sql.Date.valueOf(locDate).getTime() / DateTimeUtils.MILLIS_PER_DAY);

            locDate = LocalDate.ofEpochDay(recalculatedEpochDays);

            dateLiteral = new DateString(locDate.getYear(), locDate.getMonthValue(), locDate.getDayOfMonth());
        }

        return dateLiteral;
    }

    /** */
    private TimestampString fixOldTimestampLiteralValue(TimestampString tsLiteral) {
        long epochDirectMillis = tsLiteral.getMillisSinceEpoch();

        LocalDate locDate = LocalDate.ofInstant(Instant.ofEpochMilli(epochDirectMillis), ZoneOffset.UTC);

        if (beforeGregorian(locDate)) {
            int recalculatedEpochDays = (int)(java.sql.Date.valueOf(locDate).getTime() / DateTimeUtils.MILLIS_PER_DAY);

            locDate = LocalDate.ofEpochDay(recalculatedEpochDays);

            long timeMillisLeft = epochDirectMillis - recalculatedEpochDays * DateTimeUtils.MILLIS_PER_DAY;

            LocalTime locTime = LocalTime.ofInstant(Instant.ofEpochMilli(timeMillisLeft), ZoneOffset.UTC);

            tsLiteral = new TimestampString(locDate.getYear(), locDate.getMonthValue(), locDate.getDayOfMonth(),
                locTime.getHour(), locTime.getMinute(), locTime.getSecond()).withMillis(1000 + (int)(timeMillisLeft % 1000L));
        }

        return tsLiteral;
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
