/*
/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.java.lang;

/**
 * A java.lang.Long implementation.
 */
public class Long {

    /**
     * The smallest possible value.
     */
    public static final long MIN_VALUE = 1L << 63;

    /**
     * The largest possible value.
     */
    public static final long MAX_VALUE = (1L << 63) - 1;

    /**
     * Convert a value to a String.
     *
     * @param x the value
     * @return the String
     */
    public static String toString(long x) {
        // c: wchar_t ch[30];
        // c: swprintf(ch, 30, L"%" PRId64, x);
        // c: return STRING(ch);
        // c: return;
        if (x == MIN_VALUE) {
            return String.wrap("-9223372036854775808");
        }
        char[] ch = new char[30];
        int i = 30 - 1, count = 0;
        boolean negative;
        if (x < 0) {
            negative = true;
            x = -x;
        } else {
            negative = false;
        }
        for (; i >= 0; i--) {
            ch[i] = (char) ('0' + (x % 10));
            x /= 10;
            count++;
            if (x == 0) {
                break;
            }
        }
        if (negative) {
            ch[--i] = '-';
            count++;
        }
        return new String(ch, i, count);
    }

}
