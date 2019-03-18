/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.util.Arrays;
import org.h2.engine.Mode;
import org.h2.engine.SysProperties;
import org.h2.util.StringUtils;

/**
 * Implementation of the CHAR data type.
 */
public class ValueStringFixed extends ValueString {

    /**
     * Special value for the precision in {@link #get(String, int, Mode)} to indicate that the value
     * should <i>not</i> be trimmed.
     */
    public static final int PRECISION_DO_NOT_TRIM = Integer.MIN_VALUE;

    /**
     * Special value for the precision in {@link #get(String, int, Mode)} to indicate
     * that the default behaviour should of trimming the value should apply.
     */
    public static final int PRECISION_TRIM = -1;

    private static final ValueStringFixed EMPTY = new ValueStringFixed("");

    protected ValueStringFixed(String value) {
        super(value);
    }

    private static String trimRight(String s) {
        return trimRight(s, 0);
    }
    private static String trimRight(String s, int minLength) {
        int endIndex = s.length() - 1;
        int i = endIndex;
        while (i >= minLength && s.charAt(i) == ' ') {
            i--;
        }
        s = i == endIndex ? s : s.substring(0, i + 1);
        return s;
    }

    private static String rightPadWithSpaces(String s, int length) {
        int pad = length - s.length();
        if (pad <= 0) {
            return s;
        }
        char[] res = new char[length];
        s.getChars(0, s.length(), res, 0);
        Arrays.fill(res, s.length(), length, ' ');
        return new String(res);
    }

    @Override
    public int getType() {
        return Value.STRING_FIXED;
    }

    /**
     * Get or create a fixed length string value for the given string.
     * Spaces at the end of the string will be removed.
     *
     * @param s the string
     * @return the value
     */
    public static ValueStringFixed get(String s) {
        // Use the special precision constant PRECISION_TRIM to indicate
        // default H2 behaviour of trimming the value.
        return get(s, PRECISION_TRIM, null);
    }

    /**
     * Get or create a fixed length string value for the given string.
     * <p>
     * This method will use a {@link Mode}-specific conversion when <code>mode</code> is not
     * <code>null</code>.
     * Otherwise it will use the default H2 behaviour of trimming the given string if
     * <code>precision</code> is not {@link #PRECISION_DO_NOT_TRIM}.
     *
     * @param s the string
     * @param precision if the {@link Mode#padFixedLengthStrings} indicates that strings should
     *        be padded, this defines the overall length of the (potentially padded) string.
     *        If the special constant {@link #PRECISION_DO_NOT_TRIM} is used the value will
     *        not be trimmed.
     * @param mode the database mode
     * @return the value
     */
    public static ValueStringFixed get(String s, int precision, Mode mode) {
        // Should fixed strings be padded?
        if (mode != null && mode.padFixedLengthStrings) {
            if (precision == Integer.MAX_VALUE) {
                // CHAR without a length specification is identical to CHAR(1)
                precision = 1;
            }
            if (s.length() < precision) {
                // We have to pad
                s = rightPadWithSpaces(s, precision);
            } else {
                // We should trim, because inserting 'A   ' into a CHAR(1) is possible!
                s = trimRight(s, precision);
            }
        } else if (precision != PRECISION_DO_NOT_TRIM) {
            // Default behaviour of H2
            s = trimRight(s);
        }
        if (s.length() == 0) {
            return EMPTY;
        }
        ValueStringFixed obj = new ValueStringFixed(StringUtils.cache(s));
        if (s.length() > SysProperties.OBJECT_CACHE_MAX_PER_ELEMENT_SIZE) {
            return obj;
        }
        return (ValueStringFixed) Value.cache(obj);
    }

    @Override
    protected ValueString getNew(String s) {
        return ValueStringFixed.get(s);
    }

}
