/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.java.lang;

/**
 * A java.lang.Integer implementation.
 */
public class Integer {

    /**
     * The smallest possible value.
     */
    public static final int MIN_VALUE = 1 << 31;

    /**
     * The largest possible value.
     */
    public static final int MAX_VALUE = (int) ((1L << 31) - 1);

    /**
     * Convert a value to a String.
     *
     * @param x the value
     * @return the String
     */
    public static String toString(int x) {
        // c: wchar_t ch[20];
        // c: swprintf(ch, 20, L"%" PRId32, x);
        // c: return STRING(ch);
        // c: return;
        if (x == MIN_VALUE) {
            return String.wrap("-2147483648");
        }
        char[] ch = new char[20];
        int i = 20 - 1, count = 0;
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
