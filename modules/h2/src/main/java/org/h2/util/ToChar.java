/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Daniel Gredler
 */
package org.h2.util;

import java.math.BigDecimal;
import java.text.DateFormatSymbols;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Currency;
import java.util.Locale;
import java.util.TimeZone;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.value.Value;
import org.h2.value.ValueTimestampTimeZone;

/**
 * Emulates Oracle's TO_CHAR function.
 */
public class ToChar {

    /**
     * The beginning of the Julian calendar.
     */
    static final int JULIAN_EPOCH = -2_440_588;

    private static final int[] ROMAN_VALUES = { 1000, 900, 500, 400, 100, 90, 50, 40, 10, 9,
            5, 4, 1 };

    private static final String[] ROMAN_NUMERALS = { "M", "CM", "D", "CD", "C", "XC",
            "L", "XL", "X", "IX", "V", "IV", "I" };

    /**
     * The month field.
     */
    static final int MONTHS = 0;

    /**
     * The month field (short form).
     */
    static final int SHORT_MONTHS = 1;

    /**
     * The weekday field.
     */
    static final int WEEKDAYS = 2;

    /**
     * The weekday field (short form).
     */
    static final int SHORT_WEEKDAYS = 3;

    /**
     * The AM / PM field.
     */
    static final int AM_PM = 4;

    private static volatile String[][] NAMES;

    private ToChar() {
        // utility class
    }

    /**
     * Emulates Oracle's TO_CHAR(number) function.
     *
     * <p><table border="1">
     * <th><td>Input</td>
     * <td>Output</td>
     * <td>Closest {@link DecimalFormat} Equivalent</td></th>
     * <tr><td>,</td>
     * <td>Grouping separator.</td>
     * <td>,</td></tr>
     * <tr><td>.</td>
     * <td>Decimal separator.</td>
     * <td>.</td></tr>
     * <tr><td>$</td>
     * <td>Leading dollar sign.</td>
     * <td>$</td></tr>
     * <tr><td>0</td>
     * <td>Leading or trailing zeroes.</td>
     * <td>0</td></tr>
     * <tr><td>9</td>
     * <td>Digit.</td>
     * <td>#</td></tr>
     * <tr><td>B</td>
     * <td>Blanks integer part of a fixed point number less than 1.</td>
     * <td>#</td></tr>
     * <tr><td>C</td>
     * <td>ISO currency symbol.</td>
     * <td>\u00A4</td></tr>
     * <tr><td>D</td>
     * <td>Local decimal separator.</td>
     * <td>.</td></tr>
     * <tr><td>EEEE</td>
     * <td>Returns a value in scientific notation.</td>
     * <td>E</td></tr>
     * <tr><td>FM</td>
     * <td>Returns values with no leading or trailing spaces.</td>
     * <td>None.</td></tr>
     * <tr><td>G</td>
     * <td>Local grouping separator.</td>
     * <td>,</td></tr>
     * <tr><td>L</td>
     * <td>Local currency symbol.</td>
     * <td>\u00A4</td></tr>
     * <tr><td>MI</td>
     * <td>Negative values get trailing minus sign,
     * positive get trailing space.</td>
     * <td>-</td></tr>
     * <tr><td>PR</td>
     * <td>Negative values get enclosing angle brackets,
     * positive get spaces.</td>
     * <td>None.</td></tr>
     * <tr><td>RN</td>
     * <td>Returns values in Roman numerals.</td>
     * <td>None.</td></tr>
     * <tr><td>S</td>
     * <td>Returns values with leading/trailing +/- signs.</td>
     * <td>None.</td></tr>
     * <tr><td>TM</td>
     * <td>Returns smallest number of characters possible.</td>
     * <td>None.</td></tr>
     * <tr><td>U</td>
     * <td>Returns the dual currency symbol.</td>
     * <td>None.</td></tr>
     * <tr><td>V</td>
     * <td>Returns a value multiplied by 10^n.</td>
     * <td>None.</td></tr>
     * <tr><td>X</td>
     * <td>Hex value.</td>
     * <td>None.</td></tr>
     * </table>
     * See also TO_CHAR(number) and number format models
     * in the Oracle documentation.
     *
     * @param number the number to format
     * @param format the format pattern to use (if any)
     * @param nlsParam the NLS parameter (if any)
     * @return the formatted number
     */
    public static String toChar(BigDecimal number, String format,
            @SuppressWarnings("unused") String nlsParam) {

        // short-circuit logic for formats that don't follow common logic below
        String formatUp = format != null ? StringUtils.toUpperEnglish(format) : null;
        if (formatUp == null || formatUp.equals("TM") || formatUp.equals("TM9")) {
            String s = number.toPlainString();
            return s.startsWith("0.") ? s.substring(1) : s;
        } else if (formatUp.equals("TME")) {
            int pow = number.precision() - number.scale() - 1;
            number = number.movePointLeft(pow);
            return number.toPlainString() + "E" +
                    (pow < 0 ? '-' : '+') + (Math.abs(pow) < 10 ? "0" : "") + Math.abs(pow);
        } else if (formatUp.equals("RN")) {
            boolean lowercase = format.startsWith("r");
            String rn = StringUtils.pad(toRomanNumeral(number.intValue()), 15, " ", false);
            return lowercase ? rn.toLowerCase() : rn;
        } else if (formatUp.equals("FMRN")) {
            boolean lowercase = format.charAt(2) == 'r';
            String rn = toRomanNumeral(number.intValue());
            return lowercase ? rn.toLowerCase() : rn;
        } else if (formatUp.endsWith("X")) {
            return toHex(number, format);
        }

        String originalFormat = format;
        DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
        char localGrouping = symbols.getGroupingSeparator();
        char localDecimal = symbols.getDecimalSeparator();

        boolean leadingSign = formatUp.startsWith("S");
        if (leadingSign) {
            format = format.substring(1);
        }

        boolean trailingSign = formatUp.endsWith("S");
        if (trailingSign) {
            format = format.substring(0, format.length() - 1);
        }

        boolean trailingMinus = formatUp.endsWith("MI");
        if (trailingMinus) {
            format = format.substring(0, format.length() - 2);
        }

        boolean angleBrackets = formatUp.endsWith("PR");
        if (angleBrackets) {
            format = format.substring(0, format.length() - 2);
        }

        int v = formatUp.indexOf('V');
        if (v >= 0) {
            int digits = 0;
            for (int i = v + 1; i < format.length(); i++) {
                char c = format.charAt(i);
                if (c == '0' || c == '9') {
                    digits++;
                }
            }
            number = number.movePointRight(digits);
            format = format.substring(0, v) + format.substring(v + 1);
        }

        Integer power;
        if (format.endsWith("EEEE")) {
            power = number.precision() - number.scale() - 1;
            number = number.movePointLeft(power);
            format = format.substring(0, format.length() - 4);
        } else {
            power = null;
        }

        int maxLength = 1;
        boolean fillMode = !formatUp.startsWith("FM");
        if (!fillMode) {
            format = format.substring(2);
        }

        // blanks flag doesn't seem to actually do anything
        format = format.replaceAll("[Bb]", "");

        // if we need to round the number to fit into the format specified,
        // go ahead and do that first
        int separator = findDecimalSeparator(format);
        int formatScale = calculateScale(format, separator);
        if (formatScale < number.scale()) {
            number = number.setScale(formatScale, BigDecimal.ROUND_HALF_UP);
        }

        // any 9s to the left of the decimal separator but to the right of a
        // 0 behave the same as a 0, e.g. "09999.99" -> "00000.99"
        for (int i = format.indexOf('0'); i >= 0 && i < separator; i++) {
            if (format.charAt(i) == '9') {
                format = format.substring(0, i) + "0" + format.substring(i + 1);
            }
        }

        StringBuilder output = new StringBuilder();
        String unscaled = (number.abs().compareTo(BigDecimal.ONE) < 0 ?
                zeroesAfterDecimalSeparator(number) : "") +
                number.unscaledValue().abs().toString();

        // start at the decimal point and fill in the numbers to the left,
        // working our way from right to left
        int i = separator - 1;
        int j = unscaled.length() - number.scale() - 1;
        for (; i >= 0; i--) {
            char c = format.charAt(i);
            maxLength++;
            if (c == '9' || c == '0') {
                if (j >= 0) {
                    char digit = unscaled.charAt(j);
                    output.insert(0, digit);
                    j--;
                } else if (c == '0' && power == null) {
                    output.insert(0, '0');
                }
            } else if (c == ',') {
                // only add the grouping separator if we have more numbers
                if (j >= 0 || (i > 0 && format.charAt(i - 1) == '0')) {
                    output.insert(0, c);
                }
            } else if (c == 'G' || c == 'g') {
                // only add the grouping separator if we have more numbers
                if (j >= 0 || (i > 0 && format.charAt(i - 1) == '0')) {
                    output.insert(0, localGrouping);
                }
            } else if (c == 'C' || c == 'c') {
                Currency currency = Currency.getInstance(Locale.getDefault());
                output.insert(0, currency.getCurrencyCode());
                maxLength += 6;
            } else if (c == 'L' || c == 'l' || c == 'U' || c == 'u') {
                Currency currency = Currency.getInstance(Locale.getDefault());
                output.insert(0, currency.getSymbol());
                maxLength += 9;
            } else if (c == '$') {
                Currency currency = Currency.getInstance(Locale.getDefault());
                String cs = currency.getSymbol();
                output.insert(0, cs);
            } else {
                throw DbException.get(
                        ErrorCode.INVALID_TO_CHAR_FORMAT, originalFormat);
            }
        }

        // if the format (to the left of the decimal point) was too small
        // to hold the number, return a big "######" string
        if (j >= 0) {
            return StringUtils.pad("", format.length() + 1, "#", true);
        }

        if (separator < format.length()) {

            // add the decimal point
            maxLength++;
            char pt = format.charAt(separator);
            if (pt == 'd' || pt == 'D') {
                output.append(localDecimal);
            } else {
                output.append(pt);
            }

            // start at the decimal point and fill in the numbers to the right,
            // working our way from left to right
            i = separator + 1;
            j = unscaled.length() - number.scale();
            for (; i < format.length(); i++) {
                char c = format.charAt(i);
                maxLength++;
                if (c == '9' || c == '0') {
                    if (j < unscaled.length()) {
                        char digit = unscaled.charAt(j);
                        output.append(digit);
                        j++;
                    } else {
                        if (c == '0' || fillMode) {
                            output.append('0');
                        }
                    }
                } else {
                    throw DbException.get(
                            ErrorCode.INVALID_TO_CHAR_FORMAT, originalFormat);
                }
            }
        }

        addSign(output, number.signum(), leadingSign, trailingSign,
                trailingMinus, angleBrackets, fillMode);

        if (power != null) {
            output.append('E');
            output.append(power < 0 ? '-' : '+');
            output.append(Math.abs(power) < 10 ? "0" : "");
            output.append(Math.abs(power));
        }

        if (fillMode) {
            if (power != null) {
                output.insert(0, ' ');
            } else {
                while (output.length() < maxLength) {
                    output.insert(0, ' ');
                }
            }
        }

        return output.toString();
    }

    private static String zeroesAfterDecimalSeparator(BigDecimal number) {
        final String numberStr = number.toPlainString();
        final int idx = numberStr.indexOf('.');
        if (idx < 0) {
            return "";
        }
        int i = idx + 1;
        boolean allZeroes = true;
        for (; i < numberStr.length(); i++) {
            if (numberStr.charAt(i) != '0') {
                allZeroes = false;
                break;
            }
        }
        final char[] zeroes = new char[allZeroes ? numberStr.length() - idx - 1: i - 1 - idx];
        Arrays.fill(zeroes, '0');
        return String.valueOf(zeroes);
    }

    private static void addSign(StringBuilder output, int signum,
            boolean leadingSign, boolean trailingSign, boolean trailingMinus,
            boolean angleBrackets, boolean fillMode) {
        if (angleBrackets) {
            if (signum < 0) {
                output.insert(0, '<');
                output.append('>');
            } else if (fillMode) {
                output.insert(0, ' ');
                output.append(' ');
            }
        } else {
            String sign;
            if (signum == 0) {
                sign = "";
            } else if (signum < 0) {
                sign = "-";
            } else {
                if (leadingSign || trailingSign) {
                    sign = "+";
                } else if (fillMode) {
                    sign = " ";
                } else {
                    sign = "";
                }
            }
            if (trailingMinus || trailingSign) {
                output.append(sign);
            } else {
                output.insert(0, sign);
            }
        }
    }

    private static int findDecimalSeparator(String format) {
        int index = format.indexOf('.');
        if (index == -1) {
            index = format.indexOf('D');
            if (index == -1) {
                index = format.indexOf('d');
                if (index == -1) {
                    index = format.length();
                }
            }
        }
        return index;
    }

    private static int calculateScale(String format, int separator) {
        int scale = 0;
        for (int i = separator; i < format.length(); i++) {
            char c = format.charAt(i);
            if (c == '0' || c == '9') {
                scale++;
            }
        }
        return scale;
    }

    private static String toRomanNumeral(int number) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < ROMAN_VALUES.length; i++) {
            int value = ROMAN_VALUES[i];
            String numeral = ROMAN_NUMERALS[i];
            while (number >= value) {
                result.append(numeral);
                number -= value;
            }
        }
        return result.toString();
    }

    private static String toHex(BigDecimal number, String format) {

        boolean fillMode = !StringUtils.toUpperEnglish(format).startsWith("FM");
        boolean uppercase = !format.contains("x");
        boolean zeroPadded = format.startsWith("0");
        int digits = 0;
        for (int i = 0; i < format.length(); i++) {
            char c = format.charAt(i);
            if (c == '0' || c == 'X' || c == 'x') {
                digits++;
            }
        }

        int i = number.setScale(0, BigDecimal.ROUND_HALF_UP).intValue();
        String hex = Integer.toHexString(i);
        if (digits < hex.length()) {
            hex = StringUtils.pad("", digits + 1, "#", true);
        } else {
            if (uppercase) {
                hex = StringUtils.toUpperEnglish(hex);
            }
            if (zeroPadded) {
                hex = StringUtils.pad(hex, digits, "0", false);
            }
            if (fillMode) {
                hex = StringUtils.pad(hex, format.length() + 1, " ", false);
            }
        }

        return hex;
    }

    /**
     * Get the date (month / weekday / ...) names.
     *
     * @param names the field
     * @return the names
     */
    static String[] getDateNames(int names) {
        String[][] result = NAMES;
        if (result == null) {
            result = new String[5][];
            DateFormatSymbols dfs = DateFormatSymbols.getInstance();
            result[MONTHS] = dfs.getMonths();
            String[] months = dfs.getShortMonths();
            for (int i = 0; i < 12; i++) {
                String month = months[i];
                if (month.endsWith(".")) {
                    months[i] = month.substring(0, month.length() - 1);
                }
            }
            result[SHORT_MONTHS] = months;
            result[WEEKDAYS] = dfs.getWeekdays();
            result[SHORT_WEEKDAYS] = dfs.getShortWeekdays();
            result[AM_PM] = dfs.getAmPmStrings();
            NAMES = result;
        }
        return result[names];
    }

    /**
     * Returns time zone display name or ID for the specified date-time value.
     *
     * @param value
     *            value
     * @param tzd
     *            if {@code true} return TZD (time zone region with Daylight Saving
     *            Time information included), if {@code false} return TZR (time zone
     *            region)
     * @return time zone display name or ID
     */
    private static String getTimeZone(Value value, boolean tzd) {
        if (!(value instanceof ValueTimestampTimeZone)) {
            TimeZone tz = TimeZone.getDefault();
            if (tzd) {
                boolean daylight = tz.inDaylightTime(value.getTimestamp());
                return tz.getDisplayName(daylight, TimeZone.SHORT);
            }
            return tz.getID();
        }
        return DateTimeUtils.timeZoneNameFromOffsetMins(((ValueTimestampTimeZone) value).getTimeZoneOffsetMins());
    }

    /**
     * Emulates Oracle's TO_CHAR(datetime) function.
     *
     * <p><table border="1">
     * <th><td>Input</td>
     * <td>Output</td>
     * <td>Closest {@link SimpleDateFormat} Equivalent</td></th>
     * <tr><td>- / , . ; : "text"</td>
     * <td>Reproduced verbatim.</td>
     * <td>'text'</td></tr>
     * <tr><td>A.D. AD B.C. BC</td>
     * <td>Era designator, with or without periods.</td>
     * <td>G</td></tr>
     * <tr><td>A.M. AM P.M. PM</td>
     * <td>AM/PM marker.</td>
     * <td>a</td></tr>
     * <tr><td>CC SCC</td>
     * <td>Century.</td>
     * <td>None.</td></tr>
     * <tr><td>D</td>
     * <td>Day of week.</td>
     * <td>u</td></tr>
     * <tr><td>DAY</td>
     * <td>Name of day.</td>
     * <td>EEEE</td></tr>
     * <tr><td>DY</td>
     * <td>Abbreviated day name.</td>
     * <td>EEE</td></tr>
     * <tr><td>DD</td>
     * <td>Day of month.</td>
     * <td>d</td></tr>
     * <tr><td>DDD</td>
     * <td>Day of year.</td>
     * <td>D</td></tr>
     * <tr><td>DL</td>
     * <td>Long date format.</td>
     * <td>EEEE, MMMM d, yyyy</td></tr>
     * <tr><td>DS</td>
     * <td>Short date format.</td>
     * <td>MM/dd/yyyy</td></tr>
     * <tr><td>E</td>
     * <td>Abbreviated era name (Japanese, Chinese, Thai)</td>
     * <td>None.</td></tr>
     * <tr><td>EE</td>
     * <td>Full era name (Japanese, Chinese, Thai)</td>
     * <td>None.</td></tr>
     * <tr><td>FF[1-9]</td>
     * <td>Fractional seconds.</td>
     * <td>S</td></tr>
     * <tr><td>FM</td>
     * <td>Returns values with no leading or trailing spaces.</td>
     * <td>None.</td></tr>
     * <tr><td>FX</td>
     * <td>Requires exact matches between character data and format model.</td>
     * <td>None.</td></tr>
     * <tr><td>HH HH12</td>
     * <td>Hour in AM/PM (1-12).</td>
     * <td>hh</td></tr>
     * <tr><td>HH24</td>
     * <td>Hour in day (0-23).</td>
     * <td>HH</td></tr>
     * <tr><td>IW</td>
     * <td>Week in year.</td>
     * <td>w</td></tr>
     * <tr><td>WW</td>
     * <td>Week in year.</td>
     * <td>w</td></tr>
     * <tr><td>W</td>
     * <td>Week in month.</td>
     * <td>W</td></tr>
     * <tr><td>IYYY IYY IY I</td>
     * <td>Last 4/3/2/1 digit(s) of ISO year.</td>
     * <td>yyyy yyy yy y</td></tr>
     * <tr><td>RRRR RR</td>
     * <td>Last 4/2 digits of year.</td>
     * <td>yyyy yy</td></tr>
     * <tr><td>Y,YYY</td>
     * <td>Year with comma.</td>
     * <td>None.</td></tr>
     * <tr><td>YEAR SYEAR</td>
     * <td>Year spelled out (S prefixes BC years with minus sign).</td>
     * <td>None.</td></tr>
     * <tr><td>YYYY SYYYY</td>
     * <td>4-digit year (S prefixes BC years with minus sign).</td>
     * <td>yyyy</td></tr>
     * <tr><td>YYY YY Y</td>
     * <td>Last 3/2/1 digit(s) of year.</td>
     * <td>yyy yy y</td></tr>
     * <tr><td>J</td>
     * <td>Julian day (number of days since January 1, 4712 BC).</td>
     * <td>None.</td></tr>
     * <tr><td>MI</td>
     * <td>Minute in hour.</td>
     * <td>mm</td></tr>
     * <tr><td>MM</td>
     * <td>Month in year.</td>
     * <td>MM</td></tr>
     * <tr><td>MON</td>
     * <td>Abbreviated name of month.</td>
     * <td>MMM</td></tr>
     * <tr><td>MONTH</td>
     * <td>Name of month, padded with spaces.</td>
     * <td>MMMM</td></tr>
     * <tr><td>RM</td>
     * <td>Roman numeral month.</td>
     * <td>None.</td></tr>
     * <tr><td>Q</td>
     * <td>Quarter of year.</td>
     * <td>None.</td></tr>
     * <tr><td>SS</td>
     * <td>Seconds in minute.</td>
     * <td>ss</td></tr>
     * <tr><td>SSSSS</td>
     * <td>Seconds in day.</td>
     * <td>None.</td></tr>
     * <tr><td>TS</td>
     * <td>Short time format.</td>
     * <td>h:mm:ss aa</td></tr>
     * <tr><td>TZD</td>
     * <td>Daylight savings time zone abbreviation.</td>
     * <td>z</td></tr>
     * <tr><td>TZR</td>
     * <td>Time zone region information.</td>
     * <td>zzzz</td></tr>
     * <tr><td>X</td>
     * <td>Local radix character.</td>
     * <td>None.</td></tr>
     * </table>
     * <p>
     * See also TO_CHAR(datetime) and datetime format models
     * in the Oracle documentation.
     *
     * @param value the date-time value to format
     * @param format the format pattern to use (if any)
     * @param nlsParam the NLS parameter (if any)
     * @return the formatted timestamp
     */
    public static String toCharDateTime(Value value, String format, @SuppressWarnings("unused") String nlsParam) {
        long[] a = DateTimeUtils.dateAndTimeFromValue(value);
        long dateValue = a[0];
        long timeNanos = a[1];
        int year = DateTimeUtils.yearFromDateValue(dateValue);
        int monthOfYear = DateTimeUtils.monthFromDateValue(dateValue);
        int dayOfMonth = DateTimeUtils.dayFromDateValue(dateValue);
        int posYear = Math.abs(year);
        long second = timeNanos / 1_000_000_000;
        int nanos = (int) (timeNanos - second * 1_000_000_000);
        int minute = (int) (second / 60);
        second -= minute * 60;
        int hour = minute / 60;
        minute -= hour * 60;
        int h12 = (hour + 11) % 12 + 1;
        boolean isAM = hour < 12;
        if (format == null) {
            format = "DD-MON-YY HH.MI.SS.FF PM";
        }

        StringBuilder output = new StringBuilder();
        boolean fillMode = true;

        for (int i = 0; i < format.length();) {

            Capitalization cap;

                // AD / BC

            if ((cap = containsAt(format, i, "A.D.", "B.C.")) != null) {
                String era = year > 0 ? "A.D." : "B.C.";
                output.append(cap.apply(era));
                i += 4;
            } else if ((cap = containsAt(format, i, "AD", "BC")) != null) {
                String era = year > 0 ? "AD" : "BC";
                output.append(cap.apply(era));
                i += 2;

                // AM / PM

            } else if ((cap = containsAt(format, i, "A.M.", "P.M.")) != null) {
                String am = isAM ? "A.M." : "P.M.";
                output.append(cap.apply(am));
                i += 4;
            } else if ((cap = containsAt(format, i, "AM", "PM")) != null) {
                String am = isAM ? "AM" : "PM";
                output.append(cap.apply(am));
                i += 2;

                // Long/short date/time format

            } else if (containsAt(format, i, "DL") != null) {
                String day = getDateNames(WEEKDAYS)[DateTimeUtils.getSundayDayOfWeek(dateValue)];
                String month = getDateNames(MONTHS)[monthOfYear - 1];
                output.append(day).append(", ").append(month).append(' ').append(dayOfMonth).append(", ");
                StringUtils.appendZeroPadded(output, 4, posYear);
                i += 2;
            } else if (containsAt(format, i, "DS") != null) {
                StringUtils.appendZeroPadded(output, 2, monthOfYear);
                output.append('/');
                StringUtils.appendZeroPadded(output, 2, dayOfMonth);
                output.append('/');
                StringUtils.appendZeroPadded(output, 4, posYear);
                i += 2;
            } else if (containsAt(format, i, "TS") != null) {
                output.append(h12).append(':');
                StringUtils.appendZeroPadded(output, 2, minute);
                output.append(':');
                StringUtils.appendZeroPadded(output, 2, second);
                output.append(' ');
                output.append(getDateNames(AM_PM)[isAM ? 0 : 1]);
                i += 2;

                // Day

            } else if (containsAt(format, i, "DDD") != null) {
                output.append(DateTimeUtils.getDayOfYear(dateValue));
                i += 3;
            } else if (containsAt(format, i, "DD") != null) {
                StringUtils.appendZeroPadded(output, 2, dayOfMonth);
                i += 2;
            } else if ((cap = containsAt(format, i, "DY")) != null) {
                String day = getDateNames(SHORT_WEEKDAYS)[DateTimeUtils.getSundayDayOfWeek(dateValue)];
                output.append(cap.apply(day));
                i += 2;
            } else if ((cap = containsAt(format, i, "DAY")) != null) {
                String day = getDateNames(WEEKDAYS)[DateTimeUtils.getSundayDayOfWeek(dateValue)];
                if (fillMode) {
                    day = StringUtils.pad(day, "Wednesday".length(), " ", true);
                }
                output.append(cap.apply(day));
                i += 3;
            } else if (containsAt(format, i, "D") != null) {
                output.append(DateTimeUtils.getSundayDayOfWeek(dateValue));
                i += 1;
            } else if (containsAt(format, i, "J") != null) {
                output.append(DateTimeUtils.absoluteDayFromDateValue(dateValue) - JULIAN_EPOCH);
                i += 1;

                // Hours

            } else if (containsAt(format, i, "HH24") != null) {
                StringUtils.appendZeroPadded(output, 2, hour);
                i += 4;
            } else if (containsAt(format, i, "HH12") != null) {
                StringUtils.appendZeroPadded(output, 2, h12);
                i += 4;
            } else if (containsAt(format, i, "HH") != null) {
                StringUtils.appendZeroPadded(output, 2, h12);
                i += 2;

                // Minutes

            } else if (containsAt(format, i, "MI") != null) {
                StringUtils.appendZeroPadded(output, 2, minute);
                i += 2;

                // Seconds

            } else if (containsAt(format, i, "SSSSS") != null) {
                int seconds = (int) (timeNanos / 1_000_000_000);
                output.append(seconds);
                i += 5;
            } else if (containsAt(format, i, "SS") != null) {
                StringUtils.appendZeroPadded(output, 2, second);
                i += 2;

                // Fractional seconds

            } else if (containsAt(format, i, "FF1", "FF2",
                    "FF3", "FF4", "FF5", "FF6", "FF7", "FF8", "FF9") != null) {
                int x = format.charAt(i + 2) - '0';
                int ff = (int) (nanos * Math.pow(10, x - 9));
                StringUtils.appendZeroPadded(output, x, ff);
                i += 3;
            } else if (containsAt(format, i, "FF") != null) {
                StringUtils.appendZeroPadded(output, 9, nanos);
                i += 2;

                // Time zone

            } else if (containsAt(format, i, "TZR") != null) {
                output.append(getTimeZone(value, false));
                i += 3;
            } else if (containsAt(format, i, "TZD") != null) {
                output.append(getTimeZone(value, true));
                i += 3;

                // Week

            } else if (containsAt(format, i, "IW", "WW") != null) {
                output.append(DateTimeUtils.getWeekOfYear(dateValue, 0, 1));
                i += 2;
            } else if (containsAt(format, i, "W") != null) {
                int w = 1 + dayOfMonth / 7;
                output.append(w);
                i += 1;

                // Year

            } else if (containsAt(format, i, "Y,YYY") != null) {
                output.append(new DecimalFormat("#,###").format(posYear));
                i += 5;
            } else if (containsAt(format, i, "SYYYY") != null) {
                // Should be <= 0, but Oracle prints negative years with off-by-one difference
                if (year < 0) {
                    output.append('-');
                }
                StringUtils.appendZeroPadded(output, 4, posYear);
                i += 5;
            } else if (containsAt(format, i, "YYYY", "RRRR") != null) {
                StringUtils.appendZeroPadded(output, 4, posYear);
                i += 4;
            } else if (containsAt(format, i, "IYYY") != null) {
                StringUtils.appendZeroPadded(output, 4, Math.abs(DateTimeUtils.getIsoWeekYear(dateValue)));
                i += 4;
            } else if (containsAt(format, i, "YYY") != null) {
                StringUtils.appendZeroPadded(output, 3, posYear % 1000);
                i += 3;
            } else if (containsAt(format, i, "IYY") != null) {
                StringUtils.appendZeroPadded(output, 3, Math.abs(DateTimeUtils.getIsoWeekYear(dateValue)) % 1000);
                i += 3;
            } else if (containsAt(format, i, "YY", "RR") != null) {
                StringUtils.appendZeroPadded(output, 2, posYear % 100);
                i += 2;
            } else if (containsAt(format, i, "IY") != null) {
                StringUtils.appendZeroPadded(output, 2, Math.abs(DateTimeUtils.getIsoWeekYear(dateValue)) % 100);
                i += 2;
            } else if (containsAt(format, i, "Y") != null) {
                output.append(posYear % 10);
                i += 1;
            } else if (containsAt(format, i, "I") != null) {
                output.append(Math.abs(DateTimeUtils.getIsoWeekYear(dateValue)) % 10);
                i += 1;

                // Month / quarter

            } else if ((cap = containsAt(format, i, "MONTH")) != null) {
                String month = getDateNames(MONTHS)[monthOfYear - 1];
                if (fillMode) {
                    month = StringUtils.pad(month, "September".length(), " ", true);
                }
                output.append(cap.apply(month));
                i += 5;
            } else if ((cap = containsAt(format, i, "MON")) != null) {
                String month = getDateNames(SHORT_MONTHS)[monthOfYear - 1];
                output.append(cap.apply(month));
                i += 3;
            } else if (containsAt(format, i, "MM") != null) {
                StringUtils.appendZeroPadded(output, 2, monthOfYear);
                i += 2;
            } else if ((cap = containsAt(format, i, "RM")) != null) {
                output.append(cap.apply(toRomanNumeral(monthOfYear)));
                i += 2;
            } else if (containsAt(format, i, "Q") != null) {
                int q = 1 + ((monthOfYear - 1) / 3);
                output.append(q);
                i += 1;

                // Local radix character

            } else if (containsAt(format, i, "X") != null) {
                char c = DecimalFormatSymbols.getInstance().getDecimalSeparator();
                output.append(c);
                i += 1;

                // Format modifiers

            } else if (containsAt(format, i, "FM") != null) {
                fillMode = !fillMode;
                i += 2;
            } else if (containsAt(format, i, "FX") != null) {
                i += 2;

                // Literal text

            } else if (containsAt(format, i, "\"") != null) {
                for (i = i + 1; i < format.length(); i++) {
                    char c = format.charAt(i);
                    if (c != '"') {
                        output.append(c);
                    } else {
                        i++;
                        break;
                    }
                }
            } else if (format.charAt(i) == '-'
                    || format.charAt(i) == '/'
                    || format.charAt(i) == ','
                    || format.charAt(i) == '.'
                    || format.charAt(i) == ';'
                    || format.charAt(i) == ':'
                    || format.charAt(i) == ' ') {
                output.append(format.charAt(i));
                i += 1;

                // Anything else

            } else {
                throw DbException.get(ErrorCode.INVALID_TO_CHAR_FORMAT, format);
            }
        }

        return output.toString();
    }

    /**
     * Returns a capitalization strategy if the specified string contains any of
     * the specified substrings at the specified index. The capitalization
     * strategy indicates the casing of the substring that was found. If none of
     * the specified substrings are found, this method returns <code>null</code>
     * .
     *
     * @param s the string to check
     * @param index the index to check at
     * @param substrings the substrings to check for within the string
     * @return a capitalization strategy if the specified string contains any of
     *         the specified substrings at the specified index,
     *         <code>null</code> otherwise
     */
    private static Capitalization containsAt(String s, int index,
            String... substrings) {
        for (String substring : substrings) {
            if (index + substring.length() <= s.length()) {
                boolean found = true;
                Boolean up1 = null;
                Boolean up2 = null;
                for (int i = 0; i < substring.length(); i++) {
                    char c1 = s.charAt(index + i);
                    char c2 = substring.charAt(i);
                    if (c1 != c2 && Character.toUpperCase(c1) != Character.toUpperCase(c2)) {
                        found = false;
                        break;
                    } else if (Character.isLetter(c1)) {
                        if (up1 == null) {
                            up1 = Character.isUpperCase(c1);
                        } else if (up2 == null) {
                            up2 = Character.isUpperCase(c1);
                        }
                    }
                }
                if (found) {
                    return Capitalization.toCapitalization(up1, up2);
                }
            }
        }
        return null;
    }

    /** Represents a capitalization / casing strategy. */
    public enum Capitalization {

        /**
         * All letters are uppercased.
         */
        UPPERCASE,

        /**
         * All letters are lowercased.
         */
        LOWERCASE,

        /**
         * The string is capitalized (first letter uppercased, subsequent
         * letters lowercased).
         */
        CAPITALIZE;

        /**
         * Returns the capitalization / casing strategy which should be used
         * when the first and second letters have the specified casing.
         *
         * @param up1 whether or not the first letter is uppercased
         * @param up2 whether or not the second letter is uppercased
         * @return the capitalization / casing strategy which should be used
         *         when the first and second letters have the specified casing
         */
        static Capitalization toCapitalization(Boolean up1, Boolean up2) {
            if (up1 == null) {
                return Capitalization.CAPITALIZE;
            } else if (up2 == null) {
                return up1 ? Capitalization.UPPERCASE : Capitalization.LOWERCASE;
            } else if (up1) {
                return up2 ? Capitalization.UPPERCASE : Capitalization.CAPITALIZE;
            } else {
                return Capitalization.LOWERCASE;
            }
        }

        /**
         * Applies this capitalization strategy to the specified string.
         *
         * @param s the string to apply this strategy to
         * @return the resultant string
         */
        public String apply(String s) {
            if (s == null || s.isEmpty()) {
                return s;
            }
            switch (this) {
            case UPPERCASE:
                return StringUtils.toUpperEnglish(s);
            case LOWERCASE:
                return StringUtils.toLowerEnglish(s);
            case CAPITALIZE:
                return Character.toUpperCase(s.charAt(0)) +
                        (s.length() > 1 ? StringUtils.toLowerEnglish(s).substring(1) : "");
            default:
                throw new IllegalArgumentException(
                        "Unknown capitalization strategy: " + this);
            }
        }
    }
}
