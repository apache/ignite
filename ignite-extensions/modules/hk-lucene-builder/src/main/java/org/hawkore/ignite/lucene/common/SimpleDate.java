package org.hawkore.ignite.lucene.common;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.hawkore.ignite.lucene.IndexException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

 
// For byte-order comparability, we shift by Integer.MIN_VALUE and treat the data as an unsigned integer ranging from
// min date to max date w/epoch sitting in the center @ 2^31
public class SimpleDate
{
    private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.UTC);
    private static final long minSupportedDateMillis = TimeUnit.DAYS.toMillis(Integer.MIN_VALUE);
    private static final long maxSupportedDateMillis = TimeUnit.DAYS.toMillis(Integer.MAX_VALUE);
    private static final long maxSupportedDays = (long)Math.pow(2,32) - 1;
    private static final long byteOrderShift = (long)Math.pow(2,31) * 2;

    private static final Pattern rawPattern = Pattern.compile("^-?\\d+$");
    public static final SimpleDate instance = new SimpleDate();


    public static int dateStringToDays(String source) throws IndexException
    {
        // Raw day value in unsigned int form, epoch @ 2^31
        if (rawPattern.matcher(source).matches())
        {
            try
            {
                long result = Long.parseLong(source);

                if (result < 0 || result > maxSupportedDays)
                    throw new NumberFormatException("Input out of bounds: " + source);

                // Shift > epoch days into negative portion of Integer result for byte order comparability
                if (result >= Integer.MAX_VALUE)
                    result -= byteOrderShift;

                return (int) result;
            }
            catch (NumberFormatException e)
            {
                throw new IndexException(String.format("Unable to make unsigned int (for date) from: '%s'", source), e);
            }
        }

        // Attempt to parse as date string
        try
        {
            DateTime parsed = formatter.parseDateTime(source);
            long millis = parsed.getMillis();
            if (millis < minSupportedDateMillis)
                throw new IndexException(String.format("Input date %s is less than min supported date %s", source, new LocalDate(minSupportedDateMillis).toString()));
            if (millis > maxSupportedDateMillis)
                throw new IndexException(String.format("Input date %s is greater than max supported date %s", source, new LocalDate(maxSupportedDateMillis).toString()));

            return timeInMillisToDay(millis);
        }
        catch (IllegalArgumentException e1)
        {
            throw new IndexException(String.format("Unable to coerce '%s' to a formatted date (long)", source), e1);
        }
    }

    public static int timeInMillisToDay(long millis)
    {
        Integer result = (int) TimeUnit.MILLISECONDS.toDays(millis);
        result -= Integer.MIN_VALUE;
        return result;
    }

    public static long dayToTimeInMillis(int days)
    {
        return TimeUnit.DAYS.toMillis(days - Integer.MIN_VALUE);
    }

    public void validate(ByteBuffer bytes) throws IndexException
    {
        if (bytes.remaining() != 4)
            throw new IndexException(String.format("Expected 4 byte long for date (%d)", bytes.remaining()));
    }

    public String toString(Integer value)
    {
        if (value == null)
            return "";

        return formatter.print(new LocalDate(dayToTimeInMillis(value), DateTimeZone.UTC));
    }

    public Class<Integer> getType()
    {
        return Integer.class;
    }
}