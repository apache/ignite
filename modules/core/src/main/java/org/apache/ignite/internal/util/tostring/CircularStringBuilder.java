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

package org.apache.ignite.internal.util.tostring;

import java.util.Arrays;

/**
 * Basic string builder over circular buffer.
 */
public class CircularStringBuilder {

    /** Value */
    private final char value[];

    /** Last written character idx (-1 if empty). */
    private int finishAt = -1;

    /** Value is full flag. */
    private boolean full;

    /** Number of skipped characters */
    private int skipped = 0;

    /**
     * Creates an CircularStringBuilder of the specified capacity.
     *
     * @param capacity Capacity.
     */
    CircularStringBuilder(int capacity) {
        assert capacity > 0 : "Can't allocate CircularStringBuilder with capacity: " + capacity;

        value = new char[capacity];
    }

    /**
     * Reset internal builder state
     */
    public void reset() {
        Arrays.fill(value,(char)0);
        finishAt = -1;
        full = false;
        skipped = 0;
    }

    /**
     * Returns the length (character count).
     *
     * @return  the length of the sequence of characters currently
     *          represented by this object
     */
    public int length() { return full ? value.length : finishAt + 1; }

    /**
     * Returns the current capacity.
     *
     * @return  the current capacity
     */
    public int capacity() {
        return value.length;
    }

    /**
     * Appends the string representation of the {@code Object} argument.
     *
     * @param   obj   an {@code Object}.
     * @return  a reference to this object.
     */
    public CircularStringBuilder append(Object obj) {
        return append(String.valueOf(obj));
    }

    /**
     * Appends the specified string to this character sequence.
     *
     * @param   str   a string.
     * @return  a reference to this object.
     */
    public CircularStringBuilder append(String str) {
        if (str == null)
            return appendNull();

        int objStrLen = str.length();

        if (objStrLen >= value.length) {
            // String bigger or equal to value length
            str.getChars(objStrLen - value.length, objStrLen, value, 0);

            skipped += objStrLen - value.length + finishAt + 1;

            finishAt = value.length - 1;

            full = true;
        }
        else {
            // String smaller then value length
            if (value.length - finishAt - 1 < objStrLen) {
                // String doesn't fit into remaining part of value array
                int firstPart = value.length - finishAt - 1;

                if (firstPart > 0)
                    str.getChars(0, firstPart, value, finishAt + 1);

                str.getChars(firstPart, objStrLen, value, 0);

                skipped += full ? objStrLen : objStrLen - firstPart;

                finishAt = finishAt + objStrLen - value.length;

                full = true;
            }
            else {
                // Whole string fin into remaining part of value array
                str.getChars(0, objStrLen, value, finishAt + 1);

                skipped += full ? objStrLen : 0;

                finishAt += objStrLen;
            }
        }

        return this;
    }

    /**
     * Append StringBuffer
     *
     * @param sb StringBuffer to append.
     * @return Reference to this object.
     */
    public CircularStringBuilder append(StringBuffer sb) {
        if (sb == null)
            return appendNull();

        int len = sb.length();

        if (len < value.length)
            append(sb.toString());
        else {
            skipped += len - value.length;

            append(sb.substring(len - value.length));
        }

        return this;
    }

    /**
     * @return This builder.
     */
    private CircularStringBuilder appendNull() {
        append("null");

        return this;
    }

    /**
     * @return Count of skipped elements.
     */
    public int getSkipped() { return skipped; }

    /** {@inheritDoc} */
    @Override public String toString() {
        // Create a copy, don't share the array
        if (full) {
            char strValue[] = new char[value.length];
            int firstPart = value.length - finishAt - 1;

            System.arraycopy(value, finishAt + 1, strValue, 0, firstPart);
            System.arraycopy(value, 0, strValue, firstPart, value.length - firstPart);

            return new String(strValue, 0, strValue.length);
        }
        else
            return new String(value, 0, finishAt + 1);
    }
}
