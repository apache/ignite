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

package org.apache.ignite.internal.tostring;

import java.util.Arrays;

/**
 * Basic string builder over circular buffer.
 */
class CircularStringBuilder {
    /** Value */
    private final char[] buf;

    /** Writer position (0 if empty). */
    private int pos;

    /** Value is full flag. */
    private boolean full;

    /** Number of skipped characters */
    private int skipped;

    /**
     * Creates an CircularStringBuilder of the specified capacity.
     *
     * @param capacity Buffer capacity.
     */
    CircularStringBuilder(int capacity) {
        assert capacity > 0 : "Can't allocate CircularStringBuilder with capacity: " + capacity;

        buf = new char[capacity];
        pos = 0;
        skipped = 0;
        full = false;
    }

    /**
     * Reset internal builder state
     */
    public void reset() {
        Arrays.fill(buf, (char)0);

        pos = 0;
        full = false;
        skipped = 0;
    }

    /**
     * Returns the length (character count).
     *
     * @return the length of the sequence of characters currently
     * represented by this object
     */
    public int length() {
        return full ? buf.length : pos;
    }

    /**
     * Returns the current capacity.
     *
     * @return the current capacity
     */
    public int capacity() {
        return buf.length;
    }

    /**
     * Appends the string representation of the {@code Object} argument.
     *
     * @param obj an {@code Object}.
     * @return {@code this} for chaining.
     */
    public CircularStringBuilder append(Object obj) {
        return append(String.valueOf(obj));
    }

    /**
     * Appends the specified string to this character sequence.
     *
     * @param str a string.
     * @return {@code this} for chaining.
     */
    public CircularStringBuilder append(String str) {
        if (str == null)
            return appendNull();

        int strLen = str.length();

        if (strLen == 0)
            return this;
        else if (strLen >= buf.length) {
            // String bigger or equal to value length
            str.getChars(strLen - buf.length, strLen, buf, 0);

            skipped += strLen - buf.length + pos;

            pos = buf.length;

            full = true;
        }
        // String is shorter value length
        else if (buf.length - pos < strLen) {
            // String doesn't fit into remaining part of value array
            int firstPart = buf.length - pos;

            if (firstPart > 0)
                str.getChars(0, firstPart, buf, pos);

            str.getChars(firstPart, strLen, buf, 0);

            skipped += full ? strLen : strLen - firstPart;

            pos = pos + strLen - buf.length;

            full = true;
        }
        else {
            // Whole string fin into remaining part of value array
            str.getChars(0, strLen, buf, pos);

            skipped += full ? strLen : 0;

            pos += strLen;
        }

        return this;
    }

    /**
     * Append StringBuffer.
     *
     * @param sb StringBuffer to append.
     * @return {@code this} for chaining.
     */
    public CircularStringBuilder append(StringBuffer sb) {
        if (sb == null)
            return appendNull();

        int strLen = sb.length();

        if (strLen == 0)
            return this;
        if (strLen >= buf.length) {
            // String bigger or equal to value length
            sb.getChars(strLen - buf.length, strLen, buf, 0);

            skipped += strLen - buf.length + pos;

            pos = buf.length;

            full = true;
        }
        // String is shorter value length
        else if (buf.length - pos < strLen) {
            // String doesn't fit into remaining part of value array
            int firstPart = buf.length - pos;

            if (firstPart > 0)
                sb.getChars(0, firstPart, buf, pos);

            sb.getChars(firstPart, strLen, buf, 0);

            skipped += full ? strLen : strLen - firstPart;

            pos = pos + strLen - buf.length;

            full = true;
        }
        else {
            // Whole string fin into remaining part of value array
            sb.getChars(0, strLen, buf, pos);

            skipped += full ? strLen : 0;

            pos += strLen;
        }

        return this;
    }

    /**
     * Append StringBuilder.
     *
     * @param sb StringBuilder to append.
     * @return {@code this} for chaining.
     */
    public CircularStringBuilder append(StringBuilder sb) {
        if (sb == null)
            return appendNull();

        int strLen = sb.length();

        if (strLen == 0)
            return this;
        if (strLen >= buf.length) {
            // String bigger or equal to value length
            sb.getChars(strLen - buf.length, strLen, buf, 0);

            skipped += strLen - buf.length + pos;

            pos = buf.length;

            full = true;
        }
        // String is shorter value length
        else if (buf.length - pos < strLen) {
            // String doesn't fit into remaining part of value array
            int firstPart = buf.length - pos;

            if (firstPart > 0)
                sb.getChars(0, firstPart, buf, pos);

            sb.getChars(firstPart, strLen, buf, 0);

            skipped += full ? strLen : strLen - firstPart;

            pos = pos + strLen - buf.length;

            full = true;
        }
        else {
            // Whole string fin into remaining part of value array
            sb.getChars(0, strLen, buf, pos);

            skipped += full ? strLen : 0;

            pos += strLen;
        }

        return this;
    }

    /**
     * @return {@code this} for chaining.
     */
    private CircularStringBuilder appendNull() {
        return append("null");
    }

    /**
     * @return Count of skipped elements.
     */
    public int getSkipped() {
        return skipped;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        // Create a copy, don't share the array
        if (full && pos < buf.length) {
            char[] tmpBuf = new char[buf.length];
            int tailLen = buf.length - pos;

            System.arraycopy(buf, pos, tmpBuf, 0, tailLen);
            System.arraycopy(buf, 0, tmpBuf, tailLen, buf.length - tailLen);

            return new String(tmpBuf, 0, tmpBuf.length);
        }
        else
            return new String(buf, 0, pos);
    }
}
