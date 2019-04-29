/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import static org.h2.util.DateTimeUtils.NANOS_PER_DAY;
import static org.h2.util.DateTimeUtils.NANOS_PER_HOUR;
import static org.h2.util.DateTimeUtils.NANOS_PER_MINUTE;
import static org.h2.util.DateTimeUtils.absoluteDayFromDateValue;
import static org.h2.util.DateTimeUtils.dateAndTimeFromValue;
import static org.h2.util.DateTimeUtils.dateTimeToValue;
import static org.h2.util.DateTimeUtils.dateValueFromAbsoluteDay;
import static org.h2.util.IntervalUtils.NANOS_PER_DAY_BI;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.h2.api.ErrorCode;
import org.h2.api.IntervalQualifier;
import org.h2.engine.Session;
import org.h2.expression.function.DateTimeFunctions;
import org.h2.message.DbException;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.IntervalUtils;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueInterval;
import org.h2.value.ValueNull;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestampTimeZone;

/**
 * A mathematical operation with intervals.
 */
public class IntervalOperation extends Expression {

    public enum IntervalOpType {
        /**
         * Interval plus interval.
         */
        INTERVAL_PLUS_INTERVAL,

        /**
         * Interval minus interval.
         */
        INTERVAL_MINUS_INTERVAL,

        /**
         * Date-time plus interval.
         */
        DATETIME_PLUS_INTERVAL,

        /**
         * Date-time minus interval.
         */
        DATETIME_MINUS_INTERVAL,

        /**
         * Interval multiplied by numeric.
         */
        INTERVAL_MULTIPLY_NUMERIC,

        /**
         * Interval divided by numeric.
         */
        INTERVAL_DIVIDE_NUMERIC,

        /**
         * Date-time minus date-time.
         */
        DATETIME_MINUS_DATETIME
    }

    private final IntervalOpType opType;
    private Expression left, right;
    private TypeInfo type;

    private static BigInteger nanosFromValue(Value v) {
        long[] a = dateAndTimeFromValue(v);
        return BigInteger.valueOf(absoluteDayFromDateValue(a[0])).multiply(NANOS_PER_DAY_BI)
                .add(BigInteger.valueOf(a[1]));
    }

    public IntervalOperation(IntervalOpType opType, Expression left, Expression right) {
        this.opType = opType;
        this.left = left;
        this.right = right;
        int l = left.getType().getValueType(), r = right.getType().getValueType();
        switch (opType) {
        case INTERVAL_PLUS_INTERVAL:
        case INTERVAL_MINUS_INTERVAL:
            type = TypeInfo.getTypeInfo(Value.getHigherOrder(l, r));
            break;
        case DATETIME_PLUS_INTERVAL:
        case DATETIME_MINUS_INTERVAL:
        case INTERVAL_MULTIPLY_NUMERIC:
        case INTERVAL_DIVIDE_NUMERIC:
            type = left.getType();
            break;
        case DATETIME_MINUS_DATETIME:
            if (l == Value.TIME && r == Value.TIME) {
                type = TypeInfo.TYPE_INTERVAL_HOUR_TO_SECOND;
            } else if (l == Value.DATE && r == Value.DATE) {
                type = TypeInfo.TYPE_INTERVAL_DAY;
            } else {
                type = TypeInfo.TYPE_INTERVAL_DAY_TO_SECOND;
            }
        }
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        builder.append('(');
        left.getSQL(builder, alwaysQuote).append(' ').append(getOperationToken()).append(' ');
        return right.getSQL(builder, alwaysQuote).append(')');
    }

    private char getOperationToken() {
        switch (opType) {
        case INTERVAL_PLUS_INTERVAL:
        case DATETIME_PLUS_INTERVAL:
            return '+';
        case INTERVAL_MINUS_INTERVAL:
        case DATETIME_MINUS_INTERVAL:
        case DATETIME_MINUS_DATETIME:
            return '-';
        case INTERVAL_MULTIPLY_NUMERIC:
            return '*';
        case INTERVAL_DIVIDE_NUMERIC:
            return '/';
        default:
            throw DbException.throwInternalError("opType=" + opType);
        }
    }

    @Override
    public Value getValue(Session session) {
        Value l = left.getValue(session);
        Value r = right.getValue(session);
        if (l == ValueNull.INSTANCE || r == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        int lType = l.getValueType(), rType = r.getValueType();
        switch (opType) {
        case INTERVAL_PLUS_INTERVAL:
        case INTERVAL_MINUS_INTERVAL: {
            BigInteger a1 = IntervalUtils.intervalToAbsolute((ValueInterval) l);
            BigInteger a2 = IntervalUtils.intervalToAbsolute((ValueInterval) r);
            return IntervalUtils.intervalFromAbsolute(
                    IntervalQualifier.valueOf(Value.getHigherOrder(lType, rType) - Value.INTERVAL_YEAR),
                    opType == IntervalOpType.INTERVAL_PLUS_INTERVAL ? a1.add(a2) : a1.subtract(a2));
        }
        case DATETIME_PLUS_INTERVAL:
        case DATETIME_MINUS_INTERVAL:
            return getDateTimeWithInterval(l, r, lType, rType);
        case INTERVAL_MULTIPLY_NUMERIC:
        case INTERVAL_DIVIDE_NUMERIC: {
            BigDecimal a1 = new BigDecimal(IntervalUtils.intervalToAbsolute((ValueInterval) l));
            BigDecimal a2 = r.getBigDecimal();
            return IntervalUtils.intervalFromAbsolute(IntervalQualifier.valueOf(lType - Value.INTERVAL_YEAR),
                    (opType == IntervalOpType.INTERVAL_MULTIPLY_NUMERIC ? a1.multiply(a2) : a1.divide(a2))
                            .toBigInteger());
        }
        case DATETIME_MINUS_DATETIME:
            if (lType == Value.TIME && rType == Value.TIME) {
                long diff = ((ValueTime) l).getNanos() - ((ValueTime) r).getNanos();
                boolean negative = diff < 0;
                if (negative) {
                    diff = -diff;
                }
                return ValueInterval.from(IntervalQualifier.HOUR_TO_SECOND, negative, diff / NANOS_PER_HOUR,
                        diff % NANOS_PER_HOUR);
            } else if (lType == Value.DATE && rType == Value.DATE) {
                long diff = absoluteDayFromDateValue(((ValueDate) l).getDateValue())
                        - absoluteDayFromDateValue(((ValueDate) r).getDateValue());
                boolean negative = diff < 0;
                if (negative) {
                    diff = -diff;
                }
                return ValueInterval.from(IntervalQualifier.DAY, negative, diff, 0L);
            } else {
                BigInteger diff = nanosFromValue(l).subtract(nanosFromValue(r));
                if (lType == Value.TIMESTAMP_TZ || rType == Value.TIMESTAMP_TZ) {
                    l = l.convertTo(Value.TIMESTAMP_TZ);
                    r = r.convertTo(Value.TIMESTAMP_TZ);
                    diff = diff.add(BigInteger.valueOf((((ValueTimestampTimeZone) r).getTimeZoneOffsetMins()
                            - ((ValueTimestampTimeZone) l).getTimeZoneOffsetMins()) * NANOS_PER_MINUTE));
                }
                return IntervalUtils.intervalFromAbsolute(IntervalQualifier.DAY_TO_SECOND, diff);
            }
        }
        throw DbException.throwInternalError("type=" + opType);
    }

    private Value getDateTimeWithInterval(Value l, Value r, int lType, int rType) {
        switch (lType) {
        case Value.TIME: {
            if (DataType.isYearMonthIntervalType(rType)) {
                throw DbException.throwInternalError("type=" + rType);
            }
            BigInteger a1 = BigInteger.valueOf(((ValueTime) l).getNanos());
            BigInteger a2 = IntervalUtils.intervalToAbsolute((ValueInterval) r);
            BigInteger n = opType == IntervalOpType.DATETIME_PLUS_INTERVAL ? a1.add(a2) : a1.subtract(a2);
            if (n.signum() < 0 || n.compareTo(NANOS_PER_DAY_BI) >= 0) {
                throw DbException.get(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1, n.toString());
            }
            return ValueTime.fromNanos(n.longValue());
        }
        case Value.DATE:
        case Value.TIMESTAMP:
        case Value.TIMESTAMP_TZ:
            if (DataType.isYearMonthIntervalType(rType)) {
                long m = IntervalUtils.intervalToAbsolute((ValueInterval) r).longValue();
                if (opType == IntervalOpType.DATETIME_MINUS_INTERVAL) {
                    m = -m;
                }
                return DateTimeFunctions.dateadd("MONTH", m, l);
            } else {
                BigInteger a2 = IntervalUtils.intervalToAbsolute((ValueInterval) r);
                if (lType == Value.DATE) {
                    BigInteger a1 = BigInteger.valueOf(absoluteDayFromDateValue(((ValueDate) l).getDateValue()));
                    a2 = a2.divide(NANOS_PER_DAY_BI);
                    BigInteger n = opType == IntervalOpType.DATETIME_PLUS_INTERVAL ? a1.add(a2) : a1.subtract(a2);
                    return ValueDate.fromDateValue(dateValueFromAbsoluteDay(n.longValue()));
                } else {
                    long[] a = dateAndTimeFromValue(l);
                    long absoluteDay = absoluteDayFromDateValue(a[0]);
                    long timeNanos = a[1];
                    BigInteger[] dr = a2.divideAndRemainder(NANOS_PER_DAY_BI);
                    if (opType == IntervalOpType.DATETIME_PLUS_INTERVAL) {
                        absoluteDay += dr[0].longValue();
                        timeNanos += dr[1].longValue();
                    } else {
                        absoluteDay -= dr[0].longValue();
                        timeNanos -= dr[1].longValue();
                    }
                    if (timeNanos >= NANOS_PER_DAY) {
                        timeNanos -= NANOS_PER_DAY;
                        absoluteDay++;
                    } else if (timeNanos < 0) {
                        timeNanos += NANOS_PER_DAY;
                        absoluteDay--;
                    }
                    return dateTimeToValue(l, dateValueFromAbsoluteDay(absoluteDay), timeNanos, false);
                }
            }
        }
        throw DbException.throwInternalError("type=" + opType);
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        left.mapColumns(resolver, level, state);
        if (right != null) {
            right.mapColumns(resolver, level, state);
        }
    }

    @Override
    public Expression optimize(Session session) {
        left = left.optimize(session);
        right = right.optimize(session);
        if (left.isConstant() && right.isConstant()) {
            return ValueExpression.get(getValue(session));
        }
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        right.setEvaluatable(tableFilter, b);
    }

    @Override
    public TypeInfo getType() {
        return type;
    }

    @Override
    public void updateAggregate(Session session, int stage) {
        left.updateAggregate(session, stage);
        right.updateAggregate(session, stage);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && right.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return left.getCost() + 1 + right.getCost();
    }

    @Override
    public int getSubexpressionCount() {
        return 2;
    }

    @Override
    public Expression getSubexpression(int index) {
        switch (index) {
        case 0:
            return left;
        case 1:
            return right;
        default:
            throw new IndexOutOfBoundsException();
        }
    }

}
