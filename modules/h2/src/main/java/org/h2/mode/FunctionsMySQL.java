/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Jason Brittain (jason.brittain at gmail.com)
 */
package org.h2.mode;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.h2.util.StringUtils;

/**
 * This class implements some MySQL-specific functions.
 *
 * @author Jason Brittain
 * @author Thomas Mueller
 */
public class FunctionsMySQL {

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
     * Register the functionality in the database.
     * Nothing happens if the functions are already registered.
     *
     * @param conn the connection
     */
    public static void register(Connection conn) throws SQLException {
        String[] init = {
            "UNIX_TIMESTAMP", "unixTimestamp",
            "FROM_UNIXTIME", "fromUnixTime",
            "DATE", "date",
        };
        Statement stat = conn.createStatement();
        for (int i = 0; i < init.length; i += 2) {
            String alias = init[i], method = init[i + 1];
            stat.execute(
                    "CREATE ALIAS IF NOT EXISTS " + alias +
                    " FOR \"" + FunctionsMySQL.class.getName() + "." + method + "\"");
        }
    }

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
     * See
     * http://dev.mysql.com/doc/refman/5.1/en/date-and-time-functions.html#function_date
     * This function is dependent on the exact formatting of the MySQL date/time
     * string.
     *
     * @param dateTime The date/time String from which to extract just the date
     *            part.
     * @return the date part of the given date/time String argument.
     */
    public static String date(String dateTime) {
        if (dateTime == null) {
            return null;
        }
        int index = dateTime.indexOf(' ');
        if (index != -1) {
            return dateTime.substring(0, index);
        }
        return dateTime;
    }

}
