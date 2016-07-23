
package org.apache.ignite.internal.processors.query.h2.opt;

import java.lang.reflect.Array;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.UUID;
import org.h2.util.JdbcUtils;
import org.h2.util.Utils;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueUuid;

import static javafx.scene.input.KeyCode.T;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory.toMessage;

/**
 *
 */
@SuppressWarnings({"JavaAbbreviationUsage", "GridBracket"})
public class GridH2Utils {
    /** Copy/pasted from org.h2.util.DateTimeUtils */
    private static final int SHIFT_YEAR = 9;

    /** Copy/pasted from org.h2.util.DateTimeUtils */
    private static final int SHIFT_MONTH = 5;

    /** Static calendar. */
    private static final Calendar staticCalendar = Calendar.getInstance();

    /** */
    private static final ThreadLocal<Calendar> localCalendar = new ThreadLocal<>();

    /**
     * @return The instance of calendar for local thread.
     */
    public static Calendar getLocalCalendar() {
        Calendar res = localCalendar.get();

        if (res == null) {
            res = (Calendar)staticCalendar.clone();

            localCalendar.set(res);
        }

        return res;
    }

    /**
     * Get or create a timestamp value for the given timestamp.
     *
     * Copy/pasted from org.h2.value.ValueTimestamp#get(java.sql.Timestamp)
     *
     * @param timestamp The timestamp.
     * @return The value.
     */
    public static ValueTimestamp toValueTimestamp(Timestamp timestamp) {
        long ms = timestamp.getTime();
        long nanos = timestamp.getNanos() % 1000000;

        Calendar calendar = getLocalCalendar();

        calendar.clear();
        calendar.setTimeInMillis(ms);

        long dateValue = dateValueFromCalendar(calendar);

        nanos += nanosFromCalendar(calendar);

        return ValueTimestamp.fromDateValueAndNanos(dateValue, nanos);
    }

    /**
     * Calculate the nanoseconds since midnight from a given calendar.
     *
     * Copy/pasted from org.h2.util.DateTimeUtils#nanosFromCalendar(java.util.Calendar).
     *
     * @param cal The calendar.
     * @return Nanoseconds.
     */
    private static long nanosFromCalendar(Calendar cal) {
        int h = cal.get(Calendar.HOUR_OF_DAY);
        int m = cal.get(Calendar.MINUTE);
        int s = cal.get(Calendar.SECOND);
        int millis = cal.get(Calendar.MILLISECOND);

        return ((((((h * 60L) + m) * 60) + s) * 1000) + millis) * 1000000;
    }

    /**
     * Calculate the date value from a given calendar.
     *
     * Copy/pasted from org.h2.util.DateTimeUtils#dateValueFromCalendar(java.util.Calendar)
     *
     * @param cal The calendar.
     * @return The date value.
     */
    private static long dateValueFromCalendar(Calendar cal) {
        int year, month, day;

        year = getYear(cal);
        month = cal.get(Calendar.MONTH) + 1;
        day = cal.get(Calendar.DAY_OF_MONTH);

        return ((long) year << SHIFT_YEAR) | (month << SHIFT_MONTH) | day;
    }

    /**
     * Get the year (positive or negative) from a calendar.
     *
     * Copy/pasted from org.h2.util.DateTimeUtils#getYear(java.util.Calendar)
     *
     * @param calendar The calendar.
     * @return The year.
     */
    private static int getYear(Calendar calendar) {
        int year = calendar.get(Calendar.YEAR);

        if (calendar.get(Calendar.ERA) == GregorianCalendar.BC) {
            year = 1 - year;
        }

        return year;
    }

    public static Object getObjectFromValue(Value value) {
        switch (value.getType()) {
            case Value.NULL:
                return null;

            case Value.BOOLEAN:
                return value.getBoolean();

            case Value.BYTE:
                return value.getByte();

            case Value.SHORT:
                return value.getShort();

            case Value.INT:
                return value.getInt();

            case Value.LONG:
                return value.getLong();

            case Value.DECIMAL:
                return value.getBigDecimal();

            case Value.DOUBLE:
                return value.getDouble();

            case Value.FLOAT:
                return value.getFloat();

            case Value.DATE:
                return value.getDate();

            case Value.TIME:
                return value.getTime();

            case Value.TIMESTAMP:
                return value.getTimestamp();

            case Value.BYTES:
                return value.getBytes();

            case Value.STRING:
            case Value.STRING_FIXED:
            case Value.STRING_IGNORECASE:
                return value.getString();

            case Value.ARRAY: {
                ValueArray arr = (ValueArray)value;

                Class<?> arrType = arr.getComponentType();

                int size = arr.getList().length;

                Object res = Array.newInstance(arrType, size);

                if (!arrType.isPrimitive()) {
                    for (int i = 0; i < size; i++)
                        Array.set(res, i, getObjectFromValue(arr.getList()[i]));

                    return res;
                }

                switch (DataType.getTypeFromClass(arrType)) {
                    case Value.INT:
                        for (int i = 0; i < size; i++)
                            Array.setInt(res, i, arr.getList()[i].getInt());
                        break;

                    case Value.BYTE:
                        for (int i = 0; i < size; i++)
                            Array.setByte(res, i, arr.getList()[i].getByte());
                        break;

                    case Value.SHORT:
                        for (int i = 0; i < size; i++)
                            Array.setShort(res, i, arr.getList()[i].getShort());
                        break;

                    case Value.LONG:
                        for (int i = 0; i < size; i++)
                            Array.setLong(res, i, arr.getList()[i].getLong());
                        break;

                    case Value.DOUBLE:
                        for (int i = 0; i < size; i++)
                            Array.setDouble(res, i, arr.getList()[i].getDouble());
                        break;

                    case Value.FLOAT:
                        for (int i = 0; i < size; i++)
                            Array.setFloat(res, i, arr.getList()[i].getFloat());
                        break;

                    case Value.BOOLEAN:
                        for (int i = 0; i < size; i++)
                            Array.setBoolean(res, i, arr.getList()[i].getBoolean());
                        break;

                    default:
                        throw new IllegalArgumentException("Unexpected primitive array type [cls=" + arrType.getName() + ']');
                }

                return res;
            }

            case Value.JAVA_OBJECT:
                return JdbcUtils.deserialize(value.getBytesNoCopy(), null);

            case Value.UUID: {
                ValueUuid uuid = (ValueUuid)value;
                return new UUID(uuid.getHigh(), uuid.getLow());
            }

            default:
                throw new IllegalArgumentException("Unsupported H2 type: " + value.getType());
        }
    }
}