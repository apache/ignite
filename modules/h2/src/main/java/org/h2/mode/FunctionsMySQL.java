/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Jason Brittain (jason.brittain at gmail.com)
 */
package org.h2.mode;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;

import org.h2.api.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ValueExpression;
import org.h2.expression.function.Function;
import org.h2.expression.function.FunctionInfo;
import org.h2.message.DbException;
import org.h2.util.StringUtils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueInt;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

/**
 * This class implements some MySQL-specific functions.
 *
 * @author Jason Brittain
 * @author Thomas Mueller
 */
public class FunctionsMySQL extends FunctionsBase {

    private static final int UNIX_TIMESTAMP = 1001, FROM_UNIXTIME = 1002, DATE = 1003;

    private static final HashMap<String, FunctionInfo> FUNCTIONS = new HashMap<>();

    static {
        FUNCTIONS.put("UNIX_TIMESTAMP", new FunctionInfo("UNIX_TIMESTAMP", UNIX_TIMESTAMP,
                VAR_ARGS, Value.INT, false, false, false, true));
        FUNCTIONS.put("FROM_UNIXTIME", new FunctionInfo("FROM_UNIXTIME", FROM_UNIXTIME,
                VAR_ARGS, Value.STRING, false, true, false, true));
        FUNCTIONS.put("DATE", new FunctionInfo("DATE", DATE,
                1, Value.DATE, false, true, false, true));
    }

    /**
     * The date format of a MySQL formatted date/time.
     * Example: 2008-09-25 08:40:59
     */
    private static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /**
     * Format replacements for MySQL date formats.
     * See
     * http://dev.mysql.com/doc/refman/5.1/en/date-and-time-functions.html#function_date-format
     */
    private static final String[] FORMAT_REPLACE = {
            "%a", "EEE",
            "%b", "MMM",
            "%c", "MM",
            "%d", "dd",
            "%e", "d",
            "%H", "HH",
            "%h", "hh",
            "%I", "hh",
            "%i", "mm",
            "%j", "DDD",
            "%k", "H",
            "%l", "h",
            "%M", "MMMM",
            "%m", "MM",
            "%p", "a",
            "%r", "hh:mm:ss a",
            "%S", "ss",
            "%s", "ss",
            "%T", "HH:mm:ss",
            "%W", "EEEE",
            "%w", "F",
            "%Y", "yyyy",
            "%y", "yy",
            "%%", "%",
    };

    /**
     * Get the seconds since 1970-01-01 00:00:00 UTC.
     * See
     * http://dev.mysql.com/doc/refman/5.1/en/date-and-time-functions.html#function_unix-timestamp
     *
     * @return the current timestamp in seconds (not milliseconds).
     */
    public static int unixTimestamp() {
        return (int) (System.currentTimeMillis() / 1000L);
    }

    /**
     * Get the seconds since 1970-01-01 00:00:00 UTC of the given timestamp.
     * See
     * http://dev.mysql.com/doc/refman/5.1/en/date-and-time-functions.html#function_unix-timestamp
     *
     * @param timestamp the timestamp
     * @return the current timestamp in seconds (not milliseconds).
     */
    public static int unixTimestamp(java.sql.Timestamp timestamp) {
        return (int) (timestamp.getTime() / 1000L);
    }

    /**
     * See
     * http://dev.mysql.com/doc/refman/5.1/en/date-and-time-functions.html#function_from-unixtime
     *
     * @param seconds The current timestamp in seconds.
     * @return a formatted date/time String in the format "yyyy-MM-dd HH:mm:ss".
     */
    public static String fromUnixTime(int seconds) {
        SimpleDateFormat formatter = new SimpleDateFormat(DATE_TIME_FORMAT,
                Locale.ENGLISH);
        return formatter.format(new Date(seconds * 1000L));
    }

    /**
     * See
     * http://dev.mysql.com/doc/refman/5.1/en/date-and-time-functions.html#function_from-unixtime
     *
     * @param seconds The current timestamp in seconds.
     * @param format The format of the date/time String to return.
     * @return a formatted date/time String in the given format.
     */
    public static String fromUnixTime(int seconds, String format) {
        format = convertToSimpleDateFormat(format);
        SimpleDateFormat formatter = new SimpleDateFormat(format, Locale.ENGLISH);
        return formatter.format(new Date(seconds * 1000L));
    }

    private static String convertToSimpleDateFormat(String format) {
        String[] replace = FORMAT_REPLACE;
        for (int i = 0; i < replace.length; i += 2) {
            format = StringUtils.replaceAll(format, replace[i], replace[i + 1]);
        }
        return format;
    }

    /**
     * Returns mode-specific function for a given name, or {@code null}.
     *
     * @param database
     *            the database
     * @param upperName
     *            the upper-case name of a function
     * @return the function with specified name or {@code null}
     */
    public static Function getFunction(Database database, String upperName) {
        FunctionInfo info = FUNCTIONS.get(upperName);
        return info != null ? new FunctionsMySQL(database, info) : null;
    }

    FunctionsMySQL(Database database, FunctionInfo info) {
        super(database, info);
    }

    @Override
    protected void checkParameterCount(int len) {
        int min, max;
        switch (info.type) {
        case UNIX_TIMESTAMP:
            min = 0;
            max = 2;
            break;
        case FROM_UNIXTIME:
            min = 1;
            max = 2;
            break;
        case DATE:
            min = 1;
            max = 1;
            break;
        default:
            DbException.throwInternalError("type=" + info.type);
            return;
        }
        if (len < min || len > max) {
            throw DbException.get(ErrorCode.INVALID_PARAMETER_COUNT_2, info.name, min + ".." + max);
        }
    }

    @Override
    public Expression optimize(Session session) {
        boolean allConst = info.deterministic;
        for (int i = 0; i < args.length; i++) {
            Expression e = args[i];
            if (e == null) {
                continue;
            }
            e = e.optimize(session);
            args[i] = e;
            if (!e.isConstant()) {
                allConst = false;
            }
        }
        if (allConst) {
            return ValueExpression.get(getValue(session));
        }
        type = TypeInfo.getTypeInfo(info.returnDataType);
        return this;
    }

    @Override
    protected Value getValueWithArgs(Session session, Expression[] args) {
        Value[] values = new Value[args.length];
        Value v0 = getNullOrValue(session, args, values, 0);
        Value v1 = getNullOrValue(session, args, values, 1);
        Value result;
        switch (info.type) {
        case UNIX_TIMESTAMP:
            result = ValueInt.get(v0 == null ? unixTimestamp() : unixTimestamp(v0.getTimestamp()));
            break;
        case FROM_UNIXTIME:
            result = ValueString.get(
                    v1 == null ? fromUnixTime(v0.getInt()) : fromUnixTime(v0.getInt(), v1.getString()));
            break;
        case DATE:
            switch (v0.getValueType()) {
            case Value.DATE:
                result = v0;
                break;
            default:
                try {
                    v0 = v0.convertTo(Value.TIMESTAMP);
                } catch (DbException ex) {
                    v0 = ValueNull.INSTANCE;
                }
                //$FALL-THROUGH$
            case Value.TIMESTAMP:
            case Value.TIMESTAMP_TZ:
                result = v0.convertTo(Value.DATE);
            }
            break;
        default:
            throw DbException.throwInternalError("type=" + info.type);
        }
        return result;
    }

}
