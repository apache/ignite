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

        // Adjust temporal literals so that dates before 15.10.1582 would match our internal `long` representation.
        // The problem is that `RexLiteral#getValueAs(Class type)` uses `DateTimeUtils#ymdToUnixDate(int year, int month, int day)`.
        // It directly calculates epoch days. We use `new Date(long)`, `new Timestampt(long)`, `Date#getTime()`
        // in `TypeUtils#fromInternal(...)` and `TypeUtils#toInternal(...)`. The classic temporal types process this
        // `long` value differently for dates before Gregorian calendar. `DateTimeUtils` does not. This may cause a time shift
        // for old date literals. We fix it here because converted 'long' temporal values from date literls can bypass our
        // convertation in `TypeUtils`. Also, a long representation doesn't say whether it is already fixed in repeating calls to/from.
        // Probably, the time convertation should be simplified, unified with Calcite's manner and be kept in a single place.
        // For now, we have another temporal value convertation in `DateValueUtils`.
        if (o instanceof DateString && !(o instanceof AdjustedDateString))
            o = fixOldDateLiteralValue((DateString)o);
        else if (o instanceof TimestampString && !(o instanceof AdjustedTimestampString))
            o = fixOldDateLiteralValue((TimestampString)o);

        return super.makeLiteral(o, type, typeName);
    }

    /**
     * Changes string value of the literal to make the internal representation match the passed value before 15-10-1582.
     *
     * @see RexLiteral#getValueAs(Class)
     * @see DateTimeUtils#ymdToUnixDate(int, int, int)
     * @see TypeUtils#toLong(java.util.Date, TimeZone)
     * @see TypeUtils#fromInternal(DataContext, Object, Type)
     * @see java.util.Date#normalize(BaseCalendar.Date)
     * @see java.util.Date#getCalendarSystem(int)
     */
    private static DateString fixOldDateLiteralValue(DateString lit) {
        LocalDate locDate = LocalDate.ofEpochDay(lit.getDaysSinceEpoch());

        if (beforeGregorian(locDate)) {
            locDate = recalculateLocalDate(locDate);

            return new AdjustedDateString(lit, locDate.getYear(), locDate.getMonthValue(), locDate.getDayOfMonth());
        }

        return lit;
    }

    /** Analogue of {@link #fixOldDateLiteralValue(DateString)} for timestamp. */
    private static TimestampString fixOldDateLiteralValue(TimestampString ts) {
        long epochMillis = ts.getMillisSinceEpoch();
        long epochSeconds = epochMillis / 1000L;

        LocalDateTime dtm = LocalDateTime.ofEpochSecond(epochSeconds, 0, ZoneOffset.UTC)
            .plusNanos((int)U.millisToNanos(epochMillis - epochSeconds * 1000L));

        LocalDate dt = dtm.toLocalDate();

        if (beforeGregorian(dt)) {
            dt = recalculateLocalDate(dt);

            LocalTime tm = dtm.toLocalTime();

            return new AdjustedTimestampString(ts, dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(), tm.getHour(),
                tm.getMinute(), tm.getSecond()).withMillis((int)U.nanosToMillis(tm.getNano()));
        }

        return ts;
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

    /** */
    private static final class AdjustedDateString extends DateString {
        /** */
        private final DateString origin;

        /** */
        public AdjustedDateString(DateString origin, int y, int m, int d) {
            super(y, m, d);

            this.origin = origin;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Before-Gregorian-adopt='" + super.toString() + "', original='" + origin + '\'';
        }
    }

    /** */
    private static final class AdjustedTimestampString extends TimestampString {
        /** */
        private final TimestampString origin;

        /** */
        public AdjustedTimestampString(TimestampString origin, int y, int m, int d, int h, int mn, int s) {
            super(y, m, d, h, mn, s);

            this.origin = origin;
        }

        /** */
        private AdjustedTimestampString(TimestampString origin, String v) {
            super(v);

            this.origin = origin;
        }

        /** {@inheritDoc} */
        @Override public TimestampString withFraction(String fraction) {
            return new AdjustedTimestampString(origin, super.withFraction(fraction).toString());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Before-Gregorian-adopt='" + super.toString() + "', original='" + origin + '\'';
        }
    }
}
