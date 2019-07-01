/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Daniel Gredler
 */
package org.h2.util;

import static java.lang.String.format;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;

/**
 * Emulates Oracle's TO_DATE function.<br>
 * This class holds and handles the input data form the TO_DATE-method
 */
public class ToDateParser {
    private final String unmodifiedInputStr;
    private final String unmodifiedFormatStr;
    private final ConfigParam functionName;
    private String inputStr;
    private String formatStr;

    private boolean doyValid = false, absoluteDayValid = false,
            hour12Valid = false,
            timeZoneHMValid = false;

    private boolean bc;

    private long absoluteDay;

    private int year, month, day = 1;

    private int dayOfYear;

    private int hour, minute, second, nanos;

    private int hour12;

    private boolean isAM = true;

    private TimeZone timeZone;

    private int timeZoneHour, timeZoneMinute;

    private int currentYear, currentMonth;

    /**
     * @param input the input date with the date-time info
     * @param format the format of date-time info
     * @param functionName one of [TO_DATE, TO_TIMESTAMP] (both share the same
     *            code)
     */
    private ToDateParser(ConfigParam functionName, String input, String format) {
        this.functionName = functionName;
        inputStr = input.trim();
        // Keep a copy
        unmodifiedInputStr = inputStr;
        if (format == null || format.isEmpty()) {
            // default Oracle format.
            formatStr = functionName.getDefaultFormatStr();
        } else {
            formatStr = format.trim();
        }
        // Keep a copy
        unmodifiedFormatStr = formatStr;
    }

    private static ToDateParser getTimestampParser(ConfigParam param, String input, String format) {
        ToDateParser result = new ToDateParser(param, input, format);
        parse(result);
        return result;
    }

    private ValueTimestamp getResultingValue() {
        long dateValue;
        if (absoluteDayValid) {
            dateValue = DateTimeUtils.dateValueFromAbsoluteDay(absoluteDay);
        } else {
            int year = this.year;
            if (year == 0) {
                year = getCurrentYear();
            }
            if (bc) {
                year = 1 - year;
            }
            if (doyValid) {
                dateValue = DateTimeUtils.dateValueFromAbsoluteDay(
                        DateTimeUtils.absoluteDayFromYear(year) + dayOfYear - 1);
            } else {
                int month = this.month;
                if (month == 0) {
                    // Oracle uses current month as default
                    month = getCurrentMonth();
                }
                dateValue = DateTimeUtils.dateValue(year, month, day);
            }
        }
        int hour;
        if (hour12Valid) {
            hour = hour12 % 12;
            if (!isAM) {
                hour += 12;
            }
        } else {
            hour = this.hour;
        }
        long timeNanos = ((((hour * 60) + minute) * 60) + second) * 1_000_000_000L + nanos;
        return ValueTimestamp.fromDateValueAndNanos(dateValue, timeNanos);
    }

    private ValueTimestampTimeZone getResultingValueWithTimeZone() {
        ValueTimestamp ts = getResultingValue();
        long dateValue = ts.getDateValue();
        short offset;
        if (timeZoneHMValid) {
            offset = (short) (timeZoneHour * 60 + ((timeZoneHour >= 0) ? timeZoneMinute : -timeZoneMinute));
        } else {
            TimeZone timeZone = this.timeZone;
            if (timeZone == null) {
                timeZone = TimeZone.getDefault();
            }
            long millis = DateTimeUtils.convertDateTimeValueToMillis(timeZone, dateValue, nanos / 1_000_000);
            offset = (short) (timeZone.getOffset(millis) / 60_000);
        }
        return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, ts.getTimeNanos(), offset);
    }

    String getInputStr() {
        return inputStr;
    }

    String getFormatStr() {
        return formatStr;
    }

    String getFunctionName() {
        return functionName.name();
    }

    private void queryCurrentYearAndMonth() {
        GregorianCalendar gc = DateTimeUtils.getCalendar();
        gc.setTimeInMillis(System.currentTimeMillis());
        currentYear = gc.get(Calendar.YEAR);
        currentMonth = gc.get(Calendar.MONTH) + 1;
    }

    int getCurrentYear() {
        if (currentYear == 0) {
            queryCurrentYearAndMonth();
        }
        return currentYear;
    }

    int getCurrentMonth() {
        if (currentMonth == 0) {
            queryCurrentYearAndMonth();
        }
        return currentMonth;
    }

    void setAbsoluteDay(int absoluteDay) {
        doyValid = false;
        absoluteDayValid = true;
        this.absoluteDay = absoluteDay;
    }

    void setBC(boolean bc) {
        doyValid = false;
        absoluteDayValid = false;
        this.bc = bc;
    }

    void setYear(int year) {
        doyValid = false;
        absoluteDayValid = false;
        this.year = year;
    }

    void setMonth(int month) {
        doyValid = false;
        absoluteDayValid = false;
        this.month = month;
        if (year == 0) {
            year = 1970;
        }
    }

    void setDay(int day) {
        doyValid = false;
        absoluteDayValid = false;
        this.day = day;
        if (year == 0) {
            year = 1970;
        }
    }

    void setDayOfYear(int dayOfYear) {
        doyValid = true;
        absoluteDayValid = false;
        this.dayOfYear = dayOfYear;
    }

    void setHour(int hour) {
        hour12Valid = false;
        this.hour = hour;
    }

    void setMinute(int minute) {
        this.minute = minute;
    }

    void setSecond(int second) {
        this.second = second;
    }

    void setNanos(int nanos) {
        this.nanos = nanos;
    }

    void setAmPm(boolean isAM) {
        hour12Valid = true;
        this.isAM = isAM;
    }

    void setHour12(int hour12) {
        hour12Valid = true;
        this.hour12 = hour12;
    }

    void setTimeZone(TimeZone timeZone) {
        timeZoneHMValid = false;
        this.timeZone = timeZone;
    }

    void setTimeZoneHour(int timeZoneHour) {
        timeZoneHMValid = true;
        this.timeZoneHour = timeZoneHour;
    }

    void setTimeZoneMinute(int timeZoneMinute) {
        timeZoneHMValid = true;
        this.timeZoneMinute = timeZoneMinute;
    }

    private boolean hasToParseData() {
        return formatStr.length() > 0;
    }

    private void removeFirstChar() {
        if (!formatStr.isEmpty()) {
            formatStr = formatStr.substring(1);
        }
        if (!inputStr.isEmpty()) {
            inputStr = inputStr.substring(1);
        }
    }

    private static ToDateParser parse(ToDateParser p) {
        while (p.hasToParseData()) {
            List<ToDateTokenizer.FormatTokenEnum> tokenList =
                    ToDateTokenizer.FormatTokenEnum.getTokensInQuestion(p.getFormatStr());
            if (tokenList.isEmpty()) {
                p.removeFirstChar();
                continue;
            }
            boolean foundAnToken = false;
            for (ToDateTokenizer.FormatTokenEnum token : tokenList) {
                if (token.parseFormatStrWithToken(p)) {
                    foundAnToken = true;
                    break;
                }
            }
            if (!foundAnToken) {
                p.removeFirstChar();
            }
        }
        return p;
    }

    /**
     * Remove a token from a string.
     *
     * @param inputFragmentStr the input fragment
     * @param formatFragment the format fragment
     */
    void remove(String inputFragmentStr, String formatFragment) {
        if (inputFragmentStr != null && inputStr.length() >= inputFragmentStr.length()) {
            inputStr = inputStr.substring(inputFragmentStr.length());
        }
        if (formatFragment != null && formatStr.length() >= formatFragment.length()) {
            formatStr = formatStr.substring(formatFragment.length());
        }
    }

    @Override
    public String toString() {
        int inputStrLen = inputStr.length();
        int orgInputLen = unmodifiedInputStr.length();
        int currentInputPos = orgInputLen - inputStrLen;
        int restInputLen = inputStrLen <= 0 ? inputStrLen : inputStrLen - 1;

        int orgFormatLen = unmodifiedFormatStr.length();
        int currentFormatPos = orgFormatLen - formatStr.length();

        return format("\n    %s('%s', '%s')", functionName, unmodifiedInputStr, unmodifiedFormatStr)
                + format("\n      %s^%s ,  %s^ <-- Parsing failed at this point",
                format("%" + (functionName.name().length() + currentInputPos) + "s", ""),
                restInputLen <= 0 ? "" : format("%" + restInputLen + "s", ""),
                currentFormatPos <= 0 ? "" : format("%" + currentFormatPos + "s", ""));
    }

    /**
     * Parse a string as a timestamp with the given format.
     *
     * @param input the input
     * @param format the format
     * @return the timestamp
     */
    public static ValueTimestamp toTimestamp(String input, String format) {
        ToDateParser parser = getTimestampParser(ConfigParam.TO_TIMESTAMP, input, format);
        return parser.getResultingValue();
    }

    /**
     * Parse a string as a timestamp with the given format.
     *
     * @param input the input
     * @param format the format
     * @return the timestamp
     */
    public static ValueTimestampTimeZone toTimestampTz(String input, String format) {
        ToDateParser parser = getTimestampParser(ConfigParam.TO_TIMESTAMP_TZ, input, format);
        return parser.getResultingValueWithTimeZone();
    }

    /**
     * Parse a string as a date with the given format.
     *
     * @param input the input
     * @param format the format
     * @return the date as a timestamp
     */
    public static ValueTimestamp toDate(String input, String format) {
        ToDateParser parser = getTimestampParser(ConfigParam.TO_DATE, input, format);
        return parser.getResultingValue();
    }

    /**
     * The configuration of the date parser.
     */
    private enum ConfigParam {
        TO_DATE("DD MON YYYY"),
        TO_TIMESTAMP("DD MON YYYY HH:MI:SS"),
        TO_TIMESTAMP_TZ("DD MON YYYY HH:MI:SS TZR");

        private final String defaultFormatStr;
        ConfigParam(String defaultFormatStr) {
            this.defaultFormatStr = defaultFormatStr;
        }
        String getDefaultFormatStr() {
            return defaultFormatStr;
        }

    }

}
