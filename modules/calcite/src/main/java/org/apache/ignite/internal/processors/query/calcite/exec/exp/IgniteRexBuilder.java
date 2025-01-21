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

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.TimeZone;
import org.apache.calcite.DataContext;
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
import org.apache.ignite.internal.util.typedef.internal.U;
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

        // Adjust temporal literals so that dates before 15.10.1582 would match internal long representation.
        // `RexLiteral#getValueAs(Class type)` uses `DateTimeUtils#ymdToUnixDate(int year, int month, int day)` directly
        // calculating epoch days. We use `new Date(long)` and `new Timestampt(long)` in `TypeUtils#fromInternal(...)` and
        // and `Date#getTime()` in `TypeUtils#toInternal(...)`. Classic temporal types process the long value differently
        // for dates before Gregorian calendar. `DateTimeUtils` does not. This may cause +several weeks for old date literals.
        if (o instanceof DateString)
            o = advanceOldDateLiteralValue((DateString)o);
        else if (o instanceof TimestampString)
            o = advanceOldDateLiteralValue((TimestampString)o);

        return super.makeLiteral(o, type, typeName);
    }

    /**
     * Changes string value of the literal to make the internal representation match the passed value before 15-10-1582.
     *
     * @see RexLiteral#getValueAs(Class)
     * @see DateTimeUtils#ymdToUnixDate(int, int, int)
     * @see TypeUtils#toLong(Date, TimeZone)
     * @see TypeUtils#fromInternal(DataContext, Object, Type)
     * @see Date#normalize(BaseCalendar.Date)
     * @see Date#getCalendarSystem(int)
     */
    private DateString advanceOldDateLiteralValue(DateString lit) {
        LocalDate locDate = LocalDate.ofEpochDay(lit.getDaysSinceEpoch());

        if (beforeGregorian(locDate)) {
            locDate = recalculateLocalDate(locDate);

            lit = new DateString(locDate.getYear(), locDate.getMonthValue(), locDate.getDayOfMonth());
        }

        return lit;
    }

    /**
     * Changes string value of the literal to make the internal representation match the passed value before 15-10-1582.
     *
     * @see RexLiteral#getValueAs(Class)
     * @see DateTimeUtils#ymdToUnixDate(int, int, int)
     * @see TypeUtils#toLong(Date, TimeZone)
     * @see TypeUtils#fromInternal(DataContext, Object, Type)
     * @see Date#normalize(BaseCalendar.Date)
     * @see Date#getCalendarSystem(int)
     */
    private TimestampString advanceOldDateLiteralValue(TimestampString lit) {
        long epochMillis = lit.getMillisSinceEpoch();
        long epochSeconds = epochMillis / 1000L;

        LocalDateTime locDateTime = LocalDateTime.ofEpochSecond(epochSeconds, 0, ZoneOffset.UTC)
            .plusNanos((int)U.millisToNanos(epochMillis - epochSeconds * 1000L));

        LocalDate locDate = locDateTime.toLocalDate();

        if (beforeGregorian(locDate)) {
            locDate = recalculateLocalDate(locDate);

            LocalTime locTime = locDateTime.toLocalTime();

            lit = new TimestampString(locDate.getYear(), locDate.getMonthValue(), locDate.getDayOfMonth(),
                locTime.getHour(), locTime.getMinute(), locTime.getSecond()).withMillis((int)U.nanosToMillis(locTime.getNano()));
        }

        return lit;
    }

    /** */
    private static boolean beforeGregorian(LocalDate date) {
        if (date.getYear() > 1582)
            return false;

        if (date.getYear() < 1582)
            return true;

        return date.getMonthValue() < 10 || (date.getMonthValue() == 10 && date.getDayOfMonth() < 15);
    }

    /** */
    private static LocalDate recalculateLocalDate(LocalDate date) {
        int recalculatedEpochDays = (int)(java.sql.Date.valueOf(date).getTime() / DateTimeUtils.MILLIS_PER_DAY);

        return LocalDate.ofEpochDay(recalculatedEpochDays);
    }
}
