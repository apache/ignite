/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.lang;

import java.util.HashSet;

/**
 * Formats messages according to very simple substitution rules. Substitutions can be made 1, 2 or more arguments.
 *
 * <p>
 * For example,
 *
 * <pre>
 * LoggerMessageHelper.format(&quot;Hi {}.&quot;, &quot;there&quot;).get1();
 * </pre>
 *
 * will return the string "Hi there.".
 * <p>
 * The {} pair is called the <em>formatting anchor</em>. It serves to designate the location where arguments need to be
 * substituted within the message pattern.
 * <p>
 * In case your message contains the '{' or the '}' character, you do not have to do anything special unless the '}'
 * character immediately follows '{'. For example,
 *
 * <pre>
 * LoggerMessageHelper.format(&quot;Set {1,2,3} is not equal to {}.&quot;, &quot;1,2&quot;).get1();
 * </pre>
 *
 * will return the string "Set {1,2,3} is not equal to 1,2.".
 *
 * <p>
 * If for whatever reason you need to place the string "{}" in the message without its <em>formatting anchor</em>
 * meaning, then you need to escape the '{' character with '\', that is the backslash character. Only the '{' character
 * should be escaped. There is no need to escape the '}' character. For example,
 *
 * <pre>
 * LoggerMessageHelper.format(&quot;Set \\{} is not equal to {}.&quot;, &quot;1,2&quot;).get1();
 * </pre>
 *
 * will return the string "Set {} is not equal to 1,2.".
 *
 * <p>
 * The escaping behavior just described can be overridden by escaping the escape character '\'. Calling
 *
 * <pre>
 * LoggerMessageHelper.format(&quot;File name is C:\\\\{}.&quot;, &quot;file.zip&quot;).get1();
 * </pre>
 *
 * will return the string "File name is C:\file.zip".
 */
public final class LoggerMessageHelper {
    /** Left brace. */
    private static final char DELIM_START = '{';

    /** Formatting anchor. */
    private static final String DELIM_STR = "{}";

    /** Excape character. */
    private static final char ESCAPE_CHAR = '\\';

    /**
     * Replaces all substitutions in the messagePattern.
     *
     * @param messagePattern Message with formatting anchor.
     * @param params Parameters.
     * @return A formatted message.
     */
    public static String format(final String messagePattern, final Object... params) {
        return arrayFormat(messagePattern, params);
    }

    /**
     * Replaces all substitutions in the messagePattern. Assumes that params only contains arguments with no throwable
     * as last element.
     *
     * @param messagePattern Message with formatting anchor.
     * @param params Parameters.
     * @return A formatted message.
     */
    static String arrayFormat(final String messagePattern, final Object[] params) {
        if (messagePattern == null)
            return null;

        if (params == null)
            return messagePattern;

        int i = 0;
        int j;
        // use string builder for better multicore performance
        StringBuilder sbuf = new StringBuilder(messagePattern.length() + 50);

        int L;

        for (L = 0; L < params.length; L++) {

            j = messagePattern.indexOf(DELIM_STR, i);

            if (j == -1) {
                // no more variables
                if (i == 0) { // this is a simple string
                    return messagePattern;
                }
                else { // add the tail string which contains no variables and return
                    // the result.
                    sbuf.append(messagePattern, i, messagePattern.length());

                    return sbuf.toString();
                }
            }
            else {
                if (isEscapedDelimeter(messagePattern, j)) {
                    if (!isDoubleEscaped(messagePattern, j)) {
                        L--; // DELIM_START was escaped, thus should not be incremented

                        sbuf.append(messagePattern, i, j - 1);

                        sbuf.append(DELIM_START);

                        i = j + 1;
                    }
                    else {
                        // The escape character preceding the delimiter start is
                        // itself escaped: "abc x:\\{}"
                        // we have to consume one backward slash
                        sbuf.append(messagePattern, i, j - 1);

                        deeplyAppendParameter(sbuf, params[L], new HashSet<>());

                        i = j + 2;
                    }
                }
                else {
                    // normal case
                    sbuf.append(messagePattern, i, j);

                    deeplyAppendParameter(sbuf, params[L], new HashSet<>());

                    i = j + 2;
                }
            }
        }
        // append the characters following the last {} pair.
        sbuf.append(messagePattern, i, messagePattern.length());

        return sbuf.toString();
    }

    /**
     * Checks messagePattern has a delimiter at delimiterStartIndex position.
     *
     * @param messagePattern Message pattern.
     * @param delimiterStartIndex Checked position.
     * @return True if the char is delimiter, false otherwise.
     */
    private static boolean isEscapedDelimeter(String messagePattern, int delimiterStartIndex) {
        if (delimiterStartIndex == 0)
            return false;

        char potentialEscape = messagePattern.charAt(delimiterStartIndex - 1);

        return potentialEscape == ESCAPE_CHAR;
    }

    /**
     * Checks messagePattern has a double delimiter at delimiterStartIndex position.
     *
     * @param messagePattern Message pattern.
     * @param delimiterStartIndex Checked position.
     * @return True if a double delimiter is in this position, otherwise false.
     */
    private static boolean isDoubleEscaped(String messagePattern, int delimiterStartIndex) {
        return delimiterStartIndex >= 2 && messagePattern.charAt(delimiterStartIndex - 2) == ESCAPE_CHAR;
    }

    /**
     * Appends an object to string.
     *
     * @param sbuf Builder that contains a string for append.
     * @param o Object to append.
     * @param seenSet Set of the objects that already appended.
     */
    private static void deeplyAppendParameter(StringBuilder sbuf, Object o, HashSet<Object[]> seenSet) {
        if (o == null) {
            sbuf.append("null");

            return;
        }
        if (!o.getClass().isArray())
            safeObjectAppend(sbuf, o);
        else {
            // check for primitive array types because they
            // unfortunately cannot be cast to Object[]
            if (o instanceof boolean[])
                booleanArrayAppend(sbuf, (boolean[])o);
            else if (o instanceof byte[])
                byteArrayAppend(sbuf, (byte[])o);
            else if (o instanceof char[])
                charArrayAppend(sbuf, (char[])o);
            else if (o instanceof short[])
                shortArrayAppend(sbuf, (short[])o);
            else if (o instanceof int[])
                intArrayAppend(sbuf, (int[])o);
            else if (o instanceof long[])
                longArrayAppend(sbuf, (long[])o);
            else if (o instanceof float[])
                floatArrayAppend(sbuf, (float[])o);
            else if (o instanceof double[])
                doubleArrayAppend(sbuf, (double[])o);
            else
                objectArrayAppend(sbuf, (Object[])o, seenSet);
        }
    }

    /**
     * Appends a string representation for an object, even if the object throws exception on a {@link Object#toString()}
     * invocation.
     *
     * @param sbuf Builder that contains a string for append.
     * @param o An object that will be appended.
     */
    private static void safeObjectAppend(StringBuilder sbuf, Object o) {
        try {
            String oAsString = o.toString();

            sbuf.append(oAsString);
        }
        catch (Throwable t) {
            sbuf.append("Failed toString() invocation on an object of type [cls=" + o.getClass().getName()
                + ", errMsg=" + t.getClass().getName()
                + ", errMsg=" + t.getMessage() + ']');
        }
    }

    /**
     * Appends a object array to string.
     *
     * @param sbuf The builder contains a string before.
     * @param a Object array.
     * @param seenSet Set of the objects that already appended.
     */
    private static void objectArrayAppend(StringBuilder sbuf, Object[] a, HashSet<Object[]> seenSet) {
        sbuf.append('[');

        if (!seenSet.contains(a)) {
            seenSet.add(a);

            final int len = a.length;

            for (int i = 0; i < len; i++) {
                deeplyAppendParameter(sbuf, a[i], seenSet);

                if (i != len - 1)
                    sbuf.append(", ");
            }
            // allow repeats in siblings
            seenSet.remove(a);
        }
        else
            sbuf.append("...");

        sbuf.append(']');
    }

    /**
     * Appends an string representation of boolean array.
     *
     * @param sbuf The builder contains a string before.
     * @param a Boolean array.
     */
    private static void booleanArrayAppend(StringBuilder sbuf, boolean[] a) {
        sbuf.append('[');

        final int len = a.length;

        for (int i = 0; i < len; i++) {
            sbuf.append(a[i]);

            if (i != len - 1)
                sbuf.append(", ");
        }

        sbuf.append(']');
    }

    /**
     * Appends an string representation of byte array.
     *
     * @param sbuf The builder contains a string before.
     * @param a Byte array.
     */
    private static void byteArrayAppend(StringBuilder sbuf, byte[] a) {
        sbuf.append('[');

        final int len = a.length;

        for (int i = 0; i < len; i++) {
            sbuf.append(a[i]);

            if (i != len - 1)
                sbuf.append(", ");
        }

        sbuf.append(']');
    }

    /**
     * Appends an string representation of char array.
     *
     * @param sbuf The builder contains a string before.
     * @param a Char array.
     */
    private static void charArrayAppend(StringBuilder sbuf, char[] a) {
        sbuf.append('[');

        final int len = a.length;

        for (int i = 0; i < len; i++) {
            sbuf.append(a[i]);

            if (i != len - 1)
                sbuf.append(", ");
        }

        sbuf.append(']');
    }

    /**
     * Appends an string representation of short array.
     *
     * @param sbuf The builder contains a string before.
     * @param a Short array.
     */
    private static void shortArrayAppend(StringBuilder sbuf, short[] a) {
        sbuf.append('[');

        final int len = a.length;

        for (int i = 0; i < len; i++) {
            sbuf.append(a[i]);

            if (i != len - 1)
                sbuf.append(", ");
        }

        sbuf.append(']');
    }

    /**
     * Appends an string representation of integer array.
     *
     * @param sbuf The builder contains a string before.
     * @param a Integer array.
     */
    private static void intArrayAppend(StringBuilder sbuf, int[] a) {
        sbuf.append('[');

        final int len = a.length;

        for (int i = 0; i < len; i++) {
            sbuf.append(a[i]);

            if (i != len - 1)
                sbuf.append(", ");
        }

        sbuf.append(']');
    }

    /**
     * Appends a string representation of long array.
     *
     * @param sbuf The builder contains a string before.
     * @param a Long array.
     */
    private static void longArrayAppend(StringBuilder sbuf, long[] a) {
        sbuf.append('[');

        final int len = a.length;

        for (int i = 0; i < len; i++) {
            sbuf.append(a[i]);

            if (i != len - 1)
                sbuf.append(", ");
        }

        sbuf.append(']');
    }

    /**
     * Appends a string representation of float array.
     *
     * @param sbuf The builder contains a string before.
     * @param a Float array.
     */
    private static void floatArrayAppend(StringBuilder sbuf, float[] a) {
        sbuf.append('[');

        final int len = a.length;

        for (int i = 0; i < len; i++) {
            sbuf.append(a[i]);

            if (i != len - 1)
                sbuf.append(", ");
        }

        sbuf.append(']');
    }

    /**
     * Appends a string representation of double array.
     *
     * @param sbuf The builder contains a string before.
     * @param a Double array.
     */
    private static void doubleArrayAppend(StringBuilder sbuf, double[] a) {
        sbuf.append('[');

        final int len = a.length;

        for (int i = 0; i < len; i++) {
            sbuf.append(a[i]);

            if (i != len - 1)
                sbuf.append(", ");
        }

        sbuf.append(']');
    }
}
