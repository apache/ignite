package org.apache.ignite.internal.processors.rest.igfs.util;

import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_INSTANT;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;


public class DateUtil {
	
	public static long cpuStartTime = System.currentTimeMillis();
	
	/**
     * Alternate ISO 8601 format without fractional seconds.
     */
    static final DateTimeFormatter ALTERNATE_ISO_8601_DATE_FORMAT =
        new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
            .toFormatter()
            .withZone(UTC);

    /**
     * RFC 822 date/time formatter.
     */
    static final DateTimeFormatter RFC_822_DATE_TIME = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .parseLenient()
        .appendPattern("EEE, dd MMM yyyy HH:mm:ss")
        .appendLiteral(' ')
        .appendOffset("+HHMM", "GMT")
        .toFormatter()
        .withLocale(Locale.US)
        .withResolverStyle(ResolverStyle.SMART)
        .withChronology(IsoChronology.INSTANCE);

    // ISO_INSTANT does not handle offsets in Java 12-. See https://bugs.openjdk.java.net/browse/JDK-8166138
    private static final List<DateTimeFormatter> ALTERNATE_ISO_8601_FORMATTERS =
        Arrays.asList(ISO_INSTANT, ALTERNATE_ISO_8601_DATE_FORMAT, ISO_OFFSET_DATE_TIME);

    private static final int MILLI_SECOND_PRECISION = 3;


    /**
     * Parses the specified date string as an ISO 8601 date (yyyy-MM-dd'T'HH:mm:ss.SSSZZ)
     * and returns the {@link Instant} object.
     *
     * @param dateString
     *            The date string to parse.
     *
     * @return The parsed Instant object.
     */
    public static Instant parseIso8601Date(String dateString) {
        // For EC2 Spot Fleet.
        if (dateString.endsWith("+0000")) {
            dateString = dateString
                             .substring(0, dateString.length() - 5)
                             .concat("Z");
        }

        DateTimeParseException exception = null;

        for (DateTimeFormatter formatter : ALTERNATE_ISO_8601_FORMATTERS) {
            try {
                return parseInstant(dateString, formatter);
            } catch (DateTimeParseException e) {
                exception = e;
            }
        }

        if (exception != null) {
            throw exception;
        }

        // should never execute this
        throw new RuntimeException("Failed to parse date " + dateString);
    }

    /**
     * Formats the specified date as an ISO 8601 string.
     *
     * @param date the date to format
     * @return the ISO-8601 string representing the specified date
     */
    public static String formatIso8601Date(Instant date) {
        return ISO_INSTANT.format(date);
    }

    /**
     * Parses the specified date string as an RFC 822 date and returns the Date object.
     *
     * @param dateString
     *            The date string to parse.
     *
     * @return The parsed Date object.
     */
    public static Instant parseRfc822Date(String dateString) {
        if (dateString == null) {
            return null;
        }
        return parseInstant(dateString, RFC_822_DATE_TIME);
    }

    /**
     * Formats the specified date as an RFC 822 string.
     *
     * @param instant
     *            The instant to format.
     *
     * @return The RFC 822 string representing the specified date.
     */
    public static String formatRfc822Date(Instant instant) {
        return RFC_822_DATE_TIME.format(ZonedDateTime.ofInstant(instant, UTC));
    }

    /**
     * Parses the specified date string as an RFC 1123 date and returns the Date
     * object.
     *
     * @param dateString
     *            The date string to parse.
     *
     * @return The parsed Date object.
     */
    public static Instant parseRfc1123Date(String dateString) {
        if (dateString == null) {
            return null;
        }
        return parseInstant(dateString, RFC_1123_DATE_TIME);
    }

    /**
     * Formats the specified date as an RFC 1123 string.
     *
     * @param instant
     *            The instant to format.
     *
     * @return The RFC 1123 string representing the specified date.
     */
    public static String formatRfc1123Date(Instant instant) {
        return RFC_1123_DATE_TIME.format(ZonedDateTime.ofInstant(instant, UTC));
    }

    /**
     * Returns the number of days since epoch with respect to the given number
     * of milliseconds since epoch.
     */
    public static long numberOfDaysSinceEpoch(long milliSinceEpoch) {
        return Duration.ofMillis(milliSinceEpoch).toDays();
    }

    private static Instant parseInstant(String dateString, DateTimeFormatter formatter) {

        // Should not call formatter.withZone(ZoneOffset.UTC) because it will override the zone
        // for timestamps with an offset. See https://bugs.openjdk.java.net/browse/JDK-8177021
        if (formatter.equals(ISO_OFFSET_DATE_TIME)) {
            return formatter.parse(dateString, Instant::from);
        }

        return formatter.withZone(ZoneOffset.UTC).parse(dateString, Instant::from);
    }

    /**
     * Parses the given string containing a Unix timestamp with millisecond decimal precision into an {@link Instant} object.
     */
    public static Instant parseUnixTimestampInstant(String dateString) throws NumberFormatException {
        if (dateString == null) {
            return null;
        }
        BigDecimal dateValue = new BigDecimal(dateString);
        return Instant.ofEpochMilli(dateValue.scaleByPowerOfTen(MILLI_SECOND_PRECISION).longValue());
    }

    /**
     * Parses the given string containing a Unix timestamp in epoch millis into a {@link Instant} object.
     */
    public static Instant parseUnixTimestampMillisInstant(String dateString) throws NumberFormatException {
        if (dateString == null) {
            return null;
        }
        return Instant.ofEpochMilli(Long.parseLong(dateString));
    }

    /**
     * Formats the give {@link Instant} object into an Unix timestamp with millisecond decimal precision.
     */
    public static String formatUnixTimestampInstant(Instant instant) {
        if (instant == null) {
            return null;
        }
        BigDecimal dateValue = BigDecimal.valueOf(instant.toEpochMilli());
        return dateValue.scaleByPowerOfTen(0 - MILLI_SECOND_PRECISION)
                        .toPlainString();
    }
	
    public static String getDateFormatToSecond(Date date) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String tag = df.format(date);
        return tag;
    }
    
    public static String getDateIso8601Format(Date date) {    	
        String str = formatIso8601Date(Instant.ofEpochMilli(date.getTime()));
        return str;
    }
    
    public static String getDateGMTFormat(Date date) {
    	//DateFormat df = new SimpleDateFormat("EEE MMM ddHH:mm:ss 'GMT' yyyy",Locale.US);
        //String tag = df.format(date);
        String str = formatRfc1123Date(Instant.ofEpochMilli(date.getTime()));
        return str;
    }

    public static String getDateTagToSecond() {
        DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
        String tag = df.format(new Date());
        return tag;
    }

    public static String getUTCDateFormat() {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        String tag = df.format(new Date());
        return tag;
    }    
    
}
