/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Daniel Gredler
 */
package org.h2.util;

import static java.lang.String.format;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.h2.api.ErrorCode;
import org.h2.message.DbException;

/**
 * Emulates Oracle's TO_DATE function. This class knows all about the
 * TO_DATE-format conventions and how to parse the corresponding data.
 */
class ToDateTokenizer {

    /**
     * The pattern for a number.
     */
    static final Pattern PATTERN_INLINE = Pattern.compile("(\"[^\"]*\")");

    /**
     * The pattern for a number.
     */
    static final Pattern PATTERN_NUMBER = Pattern.compile("^([+-]?[0-9]+)");

    /**
     * The pattern for four digits (typically a year).
     */
    static final Pattern PATTERN_FOUR_DIGITS = Pattern
            .compile("^([+-]?[0-9]{4})");

    /**
     * The pattern 2-4 digits (e.g. for RRRR).
     */
    static final Pattern PATTERN_TWO_TO_FOUR_DIGITS = Pattern
            .compile("^([+-]?[0-9]{2,4})");
    /**
     * The pattern for three digits.
     */
    static final Pattern PATTERN_THREE_DIGITS = Pattern
            .compile("^([+-]?[0-9]{3})");

    /**
     * The pattern for two digits.
     */
    static final Pattern PATTERN_TWO_DIGITS = Pattern
            .compile("^([+-]?[0-9]{2})");

    /**
     * The pattern for one or two digits.
     */
    static final Pattern PATTERN_TWO_DIGITS_OR_LESS = Pattern
            .compile("^([+-]?[0-9][0-9]?)");

    /**
     * The pattern for one digit.
     */
    static final Pattern PATTERN_ONE_DIGIT = Pattern.compile("^([+-]?[0-9])");

    /**
     * The pattern for a fraction (of a second for example).
     */
    static final Pattern PATTERN_FF = Pattern.compile("^(FF[0-9]?)",
            Pattern.CASE_INSENSITIVE);

    /**
     * The pattern for "am" or "pm".
     */
    static final Pattern PATTERN_AM_PM = Pattern
            .compile("^(AM|A\\.M\\.|PM|P\\.M\\.)", Pattern.CASE_INSENSITIVE);

    /**
     * The pattern for "bc" or "ad".
     */
    static final Pattern PATTERN_BC_AD = Pattern
            .compile("^(BC|B\\.C\\.|AD|A\\.D\\.)", Pattern.CASE_INSENSITIVE);

    /**
     * The parslet for a year.
     */
    static final YearParslet PARSLET_YEAR = new YearParslet();

    /**
     * The parslet for a month.
     */
    static final MonthParslet PARSLET_MONTH = new MonthParslet();

    /**
     * The parslet for a day.
     */
    static final DayParslet PARSLET_DAY = new DayParslet();

    /**
     * The parslet for time.
     */
    static final TimeParslet PARSLET_TIME = new TimeParslet();

    /**
     * The inline parslet. E.g. 'YYYY-MM-DD"T"HH24:MI:SS"Z"' where "T" and "Z"
     * are inlined
     */
    static final InlineParslet PARSLET_INLINE = new InlineParslet();

    /**
     * Interface of the classes that can parse a specialized small bit of the
     * TO_DATE format-string.
     */
    interface ToDateParslet {

        /**
         * Parse a date part.
         *
         * @param params the parameters that contains the string
         * @param formatTokenEnum the format
         * @param formatTokenStr the format string
         */
        void parse(ToDateParser params, FormatTokenEnum formatTokenEnum,
                String formatTokenStr);
    }

    /**
     * Parslet responsible for parsing year parameter
     */
    static class YearParslet implements ToDateParslet {

        @Override
        public void parse(ToDateParser params, FormatTokenEnum formatTokenEnum,
                String formatTokenStr) {
            String inputFragmentStr = null;
            int dateNr = 0;
            switch (formatTokenEnum) {
            case SYYYY:
            case YYYY:
                inputFragmentStr = matchStringOrThrow(PATTERN_FOUR_DIGITS,
                        params, formatTokenEnum);
                dateNr = Integer.parseInt(inputFragmentStr);
                // Gregorian calendar does not have a year 0.
                // 0 = 0001 BC, -1 = 0002 BC, ... so we adjust
                if (dateNr == 0) {
                    throwException(params, "Year may not be zero");
                }
                params.setYear(dateNr >= 0 ? dateNr : dateNr + 1);
                break;
            case YYY:
                inputFragmentStr = matchStringOrThrow(PATTERN_THREE_DIGITS,
                        params, formatTokenEnum);
                dateNr = Integer.parseInt(inputFragmentStr);
                if (dateNr > 999) {
                    throwException(params, "Year may have only three digits with specified format");
                }
                dateNr += (params.getCurrentYear() / 1_000) * 1_000;
                // Gregorian calendar does not have a year 0.
                // 0 = 0001 BC, -1 = 0002 BC, ... so we adjust
                params.setYear(dateNr >= 0 ? dateNr : dateNr + 1);
                break;
            case RRRR:
                inputFragmentStr = matchStringOrThrow(
                        PATTERN_TWO_TO_FOUR_DIGITS, params, formatTokenEnum);
                dateNr = Integer.parseInt(inputFragmentStr);
                if (inputFragmentStr.length() < 4) {
                    if (dateNr < 50) {
                        dateNr += 2000;
                    } else if (dateNr < 100) {
                        dateNr += 1900;
                    }
                }
                if (dateNr == 0) {
                    throwException(params, "Year may not be zero");
                }
                params.setYear(dateNr);
                break;
            case RR:
                int cc = params.getCurrentYear() / 100;
                inputFragmentStr = matchStringOrThrow(PATTERN_TWO_DIGITS,
                        params, formatTokenEnum);
                dateNr = Integer.parseInt(inputFragmentStr) + cc * 100;
                params.setYear(dateNr);
                break;
            case EE /* NOT supported yet */:
                throwException(params, format("token '%s' not supported yet.",
                        formatTokenEnum.name()));
                break;
            case E /* NOT supported yet */:
                throwException(params, format("token '%s' not supported yet.",
                        formatTokenEnum.name()));
                break;
            case YY:
                inputFragmentStr = matchStringOrThrow(PATTERN_TWO_DIGITS,
                        params, formatTokenEnum);
                dateNr = Integer.parseInt(inputFragmentStr);
                if (dateNr > 99) {
                    throwException(params, "Year may have only two digits with specified format");
                }
                dateNr += (params.getCurrentYear() / 100) * 100;
                // Gregorian calendar does not have a year 0.
                // 0 = 0001 BC, -1 = 0002 BC, ... so we adjust
                params.setYear(dateNr >= 0 ? dateNr : dateNr + 1);
                break;
            case SCC:
            case CC:
                inputFragmentStr = matchStringOrThrow(PATTERN_TWO_DIGITS,
                        params, formatTokenEnum);
                dateNr = Integer.parseInt(inputFragmentStr) * 100;
                params.setYear(dateNr);
                break;
            case Y:
                inputFragmentStr = matchStringOrThrow(PATTERN_ONE_DIGIT, params,
                        formatTokenEnum);
                dateNr = Integer.parseInt(inputFragmentStr);
                if (dateNr > 9) {
                    throwException(params, "Year may have only two digits with specified format");
                }
                dateNr += (params.getCurrentYear() / 10) * 10;
                // Gregorian calendar does not have a year 0.
                // 0 = 0001 BC, -1 = 0002 BC, ... so we adjust
                params.setYear(dateNr >= 0 ? dateNr : dateNr + 1);
                break;
            case BC_AD:
                inputFragmentStr = matchStringOrThrow(PATTERN_BC_AD, params,
                        formatTokenEnum);
                params.setBC(inputFragmentStr.toUpperCase().startsWith("B"));
                break;
            default:
                throw new IllegalArgumentException(format(
                        "%s: Internal Error. Unhandled case: %s",
                        this.getClass().getSimpleName(), formatTokenEnum));
            }
            params.remove(inputFragmentStr, formatTokenStr);
        }
    }

    /**
     * Parslet responsible for parsing month parameter
     */
    static class MonthParslet implements ToDateParslet {
        private static final String[] ROMAN_MONTH = { "I", "II", "III", "IV",
                "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII" };

        @Override
        public void parse(ToDateParser params, FormatTokenEnum formatTokenEnum,
                String formatTokenStr) {
            String s = params.getInputStr();
            String inputFragmentStr = null;
            int dateNr = 0;
            switch (formatTokenEnum) {
            case MONTH:
                inputFragmentStr = setByName(params, ToChar.MONTHS);
                break;
            case Q /* NOT supported yet */:
                throwException(params, format("token '%s' not supported yet.",
                        formatTokenEnum.name()));
                break;
            case MON:
                inputFragmentStr = setByName(params, ToChar.SHORT_MONTHS);
                break;
            case MM:
                // Note: In Calendar Month go from 0 - 11
                inputFragmentStr = matchStringOrThrow(
                        PATTERN_TWO_DIGITS_OR_LESS, params, formatTokenEnum);
                dateNr = Integer.parseInt(inputFragmentStr);
                params.setMonth(dateNr);
                break;
            case RM:
                dateNr = 0;
                for (String monthName : ROMAN_MONTH) {
                    dateNr++;
                    int len = monthName.length();
                    if (s.length() >= len && monthName
                            .equalsIgnoreCase(s.substring(0, len))) {
                        params.setMonth(dateNr + 1);
                        inputFragmentStr = monthName;
                        break;
                    }
                }
                if (inputFragmentStr == null || inputFragmentStr.isEmpty()) {
                    throwException(params,
                            format("Issue happened when parsing token '%s'. "
                                    + "Expected one of: %s",
                                    formatTokenEnum.name(),
                                    Arrays.toString(ROMAN_MONTH)));
                }
                break;
            default:
                throw new IllegalArgumentException(format(
                        "%s: Internal Error. Unhandled case: %s",
                        this.getClass().getSimpleName(), formatTokenEnum));
            }
            params.remove(inputFragmentStr, formatTokenStr);
        }
    }

    /**
     * Parslet responsible for parsing day parameter
     */
    static class DayParslet implements ToDateParslet {
        @Override
        public void parse(ToDateParser params, FormatTokenEnum formatTokenEnum,
                String formatTokenStr) {
            String inputFragmentStr = null;
            int dateNr = 0;
            switch (formatTokenEnum) {
            case DDD:
                inputFragmentStr = matchStringOrThrow(PATTERN_NUMBER, params,
                        formatTokenEnum);
                dateNr = Integer.parseInt(inputFragmentStr);
                params.setDayOfYear(dateNr);
                break;
            case DD:
                inputFragmentStr = matchStringOrThrow(
                        PATTERN_TWO_DIGITS_OR_LESS, params, formatTokenEnum);
                dateNr = Integer.parseInt(inputFragmentStr);
                params.setDay(dateNr);
                break;
            case D:
                inputFragmentStr = matchStringOrThrow(PATTERN_ONE_DIGIT, params,
                        formatTokenEnum);
                dateNr = Integer.parseInt(inputFragmentStr);
                params.setDay(dateNr);
                break;
            case DAY:
                inputFragmentStr = setByName(params, ToChar.WEEKDAYS);
                break;
            case DY:
                inputFragmentStr = setByName(params, ToChar.SHORT_WEEKDAYS);
                break;
            case J:
                inputFragmentStr = matchStringOrThrow(PATTERN_NUMBER, params,
                        formatTokenEnum);
                dateNr = Integer.parseInt(inputFragmentStr);
                params.setAbsoluteDay(dateNr + ToChar.JULIAN_EPOCH);
                break;
            default:
                throw new IllegalArgumentException(format(
                        "%s: Internal Error. Unhandled case: %s",
                        this.getClass().getSimpleName(), formatTokenEnum));
            }
            params.remove(inputFragmentStr, formatTokenStr);
        }
    }

    /**
     * Parslet responsible for parsing time parameter
     */
    static class TimeParslet implements ToDateParslet {

        @Override
        public void parse(ToDateParser params, FormatTokenEnum formatTokenEnum,
                String formatTokenStr) {
            String inputFragmentStr = null;
            int dateNr = 0;
            switch (formatTokenEnum) {
            case HH24:
                inputFragmentStr = matchStringOrThrow(
                        PATTERN_TWO_DIGITS_OR_LESS, params, formatTokenEnum);
                dateNr = Integer.parseInt(inputFragmentStr);
                params.setHour(dateNr);
                break;
            case HH12:
            case HH:
                inputFragmentStr = matchStringOrThrow(
                        PATTERN_TWO_DIGITS_OR_LESS, params, formatTokenEnum);
                dateNr = Integer.parseInt(inputFragmentStr);
                params.setHour12(dateNr);
                break;
            case MI:
                inputFragmentStr = matchStringOrThrow(
                        PATTERN_TWO_DIGITS_OR_LESS, params, formatTokenEnum);
                dateNr = Integer.parseInt(inputFragmentStr);
                params.setMinute(dateNr);
                break;
            case SS:
                inputFragmentStr = matchStringOrThrow(
                        PATTERN_TWO_DIGITS_OR_LESS, params, formatTokenEnum);
                dateNr = Integer.parseInt(inputFragmentStr);
                params.setSecond(dateNr);
                break;
            case SSSSS: {
                inputFragmentStr = matchStringOrThrow(PATTERN_NUMBER, params,
                        formatTokenEnum);
                dateNr = Integer.parseInt(inputFragmentStr);
                int second = dateNr % 60;
                dateNr /= 60;
                int minute = dateNr % 60;
                dateNr /= 60;
                int hour = dateNr % 24;
                params.setHour(hour);
                params.setMinute(minute);
                params.setSecond(second);
                break;
            }
            case FF:
                inputFragmentStr = matchStringOrThrow(PATTERN_NUMBER, params,
                        formatTokenEnum);
                String paddedRightNrStr = format("%-9s", inputFragmentStr)
                        .replace(' ', '0');
                paddedRightNrStr = paddedRightNrStr.substring(0, 9);
                double nineDigits = Double.parseDouble(paddedRightNrStr);
                params.setNanos((int) nineDigits);
                break;
            case AM_PM:
                inputFragmentStr = matchStringOrThrow(PATTERN_AM_PM, params,
                        formatTokenEnum);
                if (inputFragmentStr.toUpperCase().startsWith("A")) {
                    params.setAmPm(true);
                } else {
                    params.setAmPm(false);
                }
                break;
            case TZH:
                inputFragmentStr = matchStringOrThrow(
                        PATTERN_TWO_DIGITS_OR_LESS, params, formatTokenEnum);
                dateNr = Integer.parseInt(inputFragmentStr);
                params.setTimeZoneHour(dateNr);
                break;
            case TZM:
                inputFragmentStr = matchStringOrThrow(
                        PATTERN_TWO_DIGITS_OR_LESS, params, formatTokenEnum);
                dateNr = Integer.parseInt(inputFragmentStr);
                params.setTimeZoneMinute(dateNr);
                break;
            case TZR:
            case TZD:
                String tzName = params.getInputStr();
                params.setTimeZone(TimeZone.getTimeZone(tzName));
                inputFragmentStr = tzName;
                break;
            default:
                throw new IllegalArgumentException(format(
                        "%s: Internal Error. Unhandled case: %s",
                        this.getClass().getSimpleName(), formatTokenEnum));
            }
            params.remove(inputFragmentStr, formatTokenStr);
        }
    }

    /**
     * Parslet responsible for parsing year parameter
     */
    static class InlineParslet implements ToDateParslet {
        @Override
        public void parse(ToDateParser params, FormatTokenEnum formatTokenEnum,
                String formatTokenStr) {
            String inputFragmentStr = null;
            switch (formatTokenEnum) {
            case INLINE:
                inputFragmentStr = formatTokenStr.replace("\"", "");
                break;
            default:
                throw new IllegalArgumentException(format(
                        "%s: Internal Error. Unhandled case: %s",
                        this.getClass().getSimpleName(), formatTokenEnum));
            }
            params.remove(inputFragmentStr, formatTokenStr);
        }

    }

    /**
     * Match the pattern, or if not possible throw an exception.
     *
     * @param p the pattern
     * @param params the parameters with the input string
     * @param aEnum the pattern name
     * @return the matched value
     */
    static String matchStringOrThrow(Pattern p, ToDateParser params,
            Enum<?> aEnum) {
        String s = params.getInputStr();
        Matcher matcher = p.matcher(s);
        if (!matcher.find()) {
            throwException(params, format(
                    "Issue happened when parsing token '%s'", aEnum.name()));
        }
        return matcher.group(1);
    }

    /**
     * Set the given field in the calendar.
     *
     * @param params the parameters with the input string
     * @param field the field to set
     * @return the matched value
     */
    static String setByName(ToDateParser params, int field) {
        String inputFragmentStr = null;
        String s = params.getInputStr();
        String[] values = ToChar.getDateNames(field);
        for (int i = 0; i < values.length; i++) {
            String dayName = values[i];
            if (dayName == null) {
                continue;
            }
            int len = dayName.length();
            if (dayName.equalsIgnoreCase(s.substring(0, len))) {
                switch (field) {
                case ToChar.MONTHS:
                case ToChar.SHORT_MONTHS:
                    params.setMonth(i + 1);
                    break;
                case ToChar.WEEKDAYS:
                case ToChar.SHORT_WEEKDAYS:
                    // TODO
                    break;
                default:
                    throw new IllegalArgumentException();
                }
                inputFragmentStr = dayName;
                break;
            }
        }
        if (inputFragmentStr == null || inputFragmentStr.isEmpty()) {
            throwException(params, format(
                    "Tried to parse one of '%s' but failed (may be an internal error?)",
                    Arrays.toString(values)));
        }
        return inputFragmentStr;
    }

    /**
     * Throw a parse exception.
     *
     * @param params the parameters with the input string
     * @param errorStr the error string
     */
    static void throwException(ToDateParser params, String errorStr) {
        throw DbException.get(ErrorCode.INVALID_TO_DATE_FORMAT,
                params.getFunctionName(),
                format(" %s. Details: %s", errorStr, params));
    }

    /**
     * The format tokens.
     */
    public enum FormatTokenEnum {
        // 4-digit year
        YYYY(PARSLET_YEAR),
        // 4-digit year with sign (- = B.C.)
        SYYYY(PARSLET_YEAR),
        // 3-digit year
        YYY(PARSLET_YEAR),
        // 2-digit year
        YY(PARSLET_YEAR),
        // Two-digit century with with sign (- = B.C.)
        SCC(PARSLET_YEAR),
        // Two-digit century.
        CC(PARSLET_YEAR),
        // 2-digit -> 4-digit year 0-49 -> 20xx , 50-99 -> 19xx
        RRRR(PARSLET_YEAR),
        // last 2-digit of the year using "current" century value.
        RR(PARSLET_YEAR),
        // Meridian indicator
        BC_AD(PARSLET_YEAR, PATTERN_BC_AD),
        // Full Name of month
        MONTH(PARSLET_MONTH),
        // Abbreviated name of month.
        MON(PARSLET_MONTH),
        // Month (01-12; JAN = 01).
        MM(PARSLET_MONTH),
        // Roman numeral month (I-XII; JAN = I).
        RM(PARSLET_MONTH),
        // Day of year (1-366).
        DDD(PARSLET_DAY),
        // Name of day.
        DAY(PARSLET_DAY),
        // Day of month (1-31).
        DD(PARSLET_DAY),
        // Abbreviated name of day.
        DY(PARSLET_DAY), HH24(PARSLET_TIME), HH12(PARSLET_TIME),
        // Hour of day (1-12).
        HH(PARSLET_TIME),
        // Min
        MI(PARSLET_TIME),
        // Seconds past midnight (0-86399)
        SSSSS(PARSLET_TIME), SS(PARSLET_TIME),
        // Fractional seconds
        FF(PARSLET_TIME, PATTERN_FF),
        // Time zone hour.
        TZH(PARSLET_TIME),
        // Time zone minute.
        TZM(PARSLET_TIME),
        // Time zone region ID
        TZR(PARSLET_TIME),
        // Daylight savings information. Example:
        // PST (for US/Pacific standard time);
        TZD(PARSLET_TIME),
        // Meridian indicator
        AM_PM(PARSLET_TIME, PATTERN_AM_PM),
        // NOT supported yet -
        // Full era name (Japanese Imperial, ROC Official,
        // and Thai Buddha calendars).
        EE(PARSLET_YEAR),
        // NOT supported yet -
        // Abbreviated era name (Japanese Imperial,
        // ROC Official, and Thai Buddha calendars).
        E(PARSLET_YEAR), Y(PARSLET_YEAR),
        // Quarter of year (1, 2, 3, 4; JAN-MAR = 1).
        Q(PARSLET_MONTH),
        // Day of week (1-7).
        D(PARSLET_DAY),
        // NOT supported yet -
        // Julian day; the number of days since Jan 1, 4712 BC.
        J(PARSLET_DAY),
        // Inline text e.g. to_date('2017-04-21T00:00:00Z',
        // 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
        // where "T" and "Z" are inlined
        INLINE(PARSLET_INLINE, PATTERN_INLINE);

        private static final List<FormatTokenEnum> EMPTY_LIST = Collections
                .emptyList();

        private static final Map<Character, List<FormatTokenEnum>> CACHE = new HashMap<>(
                FormatTokenEnum.values().length);
        private final ToDateParslet toDateParslet;
        private final Pattern patternToUse;

        /**
         * Construct a format token.
         *
         * @param toDateParslet the date parslet
         * @param patternToUse the pattern
         */
        FormatTokenEnum(ToDateParslet toDateParslet, Pattern patternToUse) {
            this.toDateParslet = toDateParslet;
            this.patternToUse = patternToUse;
        }

        /**
         * Construct a format token.
         *
         * @param toDateParslet the date parslet
         */
        FormatTokenEnum(ToDateParslet toDateParslet) {
            this.toDateParslet = toDateParslet;
            patternToUse = Pattern.compile(format("^(%s)", name()),
                    Pattern.CASE_INSENSITIVE);
        }

        /**
         * Optimization: Only return a list of {@link FormatTokenEnum} that
         * share the same 1st char using the 1st char of the 'to parse'
         * formatStr. Or return empty list if no match.
         *
         * @param formatStr the format string
         * @return the list of tokens
         */
        static List<FormatTokenEnum> getTokensInQuestion(String formatStr) {
            List<FormatTokenEnum> result = EMPTY_LIST;
            if (CACHE.size() <= 0) {
                initCache();
            }
            if (formatStr != null && formatStr.length() > 0) {
                Character key = Character.toUpperCase(formatStr.charAt(0));
                switch (key) {
                case '"':
                    result = new ArrayList<>();
                    result.add(INLINE);
                    break;
                default:
                    result = CACHE.get(key);
                }
            }
            if (result == null) {
                result = EMPTY_LIST;
            }
            return result;
        }

        private static synchronized void initCache() {
            if (CACHE.size() <= 0) {
                for (FormatTokenEnum token : FormatTokenEnum.values()) {

                    List<Character> tokenKeys = new ArrayList<>();

                    if (token.name().contains("_")) {
                        String[] tokens = token.name().split("_");
                        for (String tokenLets : tokens) {
                            tokenKeys.add(tokenLets.toUpperCase().charAt(0));
                        }
                    } else {
                        tokenKeys.add(token.name().toUpperCase().charAt(0));
                    }

                    for (Character tokenKey : tokenKeys) {
                        List<FormatTokenEnum> l = CACHE.get(tokenKey);
                        if (l == null) {
                            l = new ArrayList<>(1);
                            CACHE.put(tokenKey, l);
                        }
                        l.add(token);
                    }
                }
            }

        }

        /**
         * Parse the format-string with passed token of {@link FormatTokenEnum}.
         * If token matches return true, otherwise false.
         *
         * @param params the parameters
         * @return true if it matches
         */
        boolean parseFormatStrWithToken(ToDateParser params) {
            Matcher matcher = patternToUse.matcher(params.getFormatStr());
            boolean foundToken = matcher.find();
            if (foundToken) {
                String formatTokenStr = matcher.group(1);
                toDateParslet.parse(params, this, formatTokenStr);
            }
            return foundToken;
        }
    }

}
