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
     * Returns the length (character count).
     *
     * @return  the length of the sequence of characters currently
     *          represented by this object
     */
    public int length() {
        return full ? value.length : finishAt + 1;
    }

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
     * Inserts a string into the buffer at the specified logical offset.
     * This method is optimized to minimize the number of elements moved by choosing
     * to shift elements from the closest end (left or right) to the insertion point.
     *
     * @param offset      The logical position (accounting for skipped characters)
     *                    at which to insert.
     * @param valToInsert The string to be inserted.
     * @throws StringIndexOutOfBoundsException if the offset is invalid.
     */
    public void insert(int offset, String valToInsert) {
        int curLength = length();
        int offsetInsideBuf = offset - skipped;
        if (offset < 0 || offsetInsideBuf > curLength)
            throw new StringIndexOutOfBoundsException("Offset " + offset + " out of bounds for length " + curLength);
        if (valToInsert == null)
            valToInsert = "null";
        int insertLength = valToInsert.length();
        if (insertLength == 0)
            return;
        if (offsetInsideBuf == curLength) {
            append(valToInsert);
            return;
        }
        int spareSpace = value.length - curLength;
        int insertCnt = Math.min(valToInsert.length(), spareSpace + offsetInsideBuf);
        if (insertCnt <= 0) {
            skipped += valToInsert.length();
            return;
        }
        int bufStartShiftedOffset = full ? (finishAt + 1) % value.length : 0;
        int shiftedOffset = (bufStartShiftedOffset + offsetInsideBuf) % value.length;
        int moveRightCnt = ((shiftedOffset <= finishAt ? 0 : curLength) + finishAt + 1) - shiftedOffset;
        if (!full || offset - skipped > curLength / 2) {
            shiftRight(insertCnt, moveRightCnt);
            int charsToSkip = Math.max(0, insertCnt - spareSpace);
            finishAt = (finishAt + insertCnt) % value.length;
            shiftedOffset = (shiftedOffset + insertCnt) % value.length;
            full = curLength + insertCnt >= value.length;
            insertStringTail(valToInsert, shiftedOffset, insertCnt);
            skipped += charsToSkip;
        }
        else {
            int moveLeftCnt = (curLength - moveRightCnt - insertCnt);
            shiftLeft(insertCnt, moveLeftCnt);
            insertStringTail(valToInsert, shiftedOffset, insertCnt);
            skipped += valToInsert.length();
        }
    }

    /**
     * @return Count of skipped elements.
     */
    public int getSkipped() {
        return skipped;
    }

    /**
     * Returns a substring from the logical sequence of characters, accounting for
     * the circular buffer structure and any skipped characters.
     *
     * <p>This method first validates the indices against the total logical length
     * (skipped + visible characters). If the requested range is empty or fully within
     * the skipped portion, an empty string is returned for efficiency.
     *
     * <p>It then calculates the physical indices in the internal array. If the
     * substring wraps around the end of the circular buffer, it performs a two-part
     * copy operation to assemble the result.
     *
     * @param beginIdx the beginning index, inclusive.
     * @param endIdx the ending index, exclusive.
     * @return a new String containing the specified subsequence.
     * @throws StringIndexOutOfBoundsException if beginIdx or endIdx are negative,
     *         or if endIdx is greater than the total logical length.
     * @throws IllegalArgumentException if beginIdx is greater than endIdx.
     */
    public String substring(int beginIdx, int endIdx) {
        if (beginIdx < 0 || endIdx < 0 || endIdx > skipped + length())
            throw new StringIndexOutOfBoundsException(
                    "Some of indexes is out of bounds. Begind index = " + beginIdx + " end index = " + endIdx);
        if (beginIdx > endIdx)
            throw new IllegalArgumentException(
                    "Begin index can not be greater then end index (begin = " + beginIdx + " end = " + endIdx + ")");
        if (endIdx <= skipped || beginIdx == endIdx) return "";
        char resultArr[] = new char[Math.max(skipped, endIdx) - Math.max(skipped, beginIdx)];
        int effectiveBeginIdx = ((full ? (finishAt + 1) : 0) + Math.max(skipped, beginIdx) - skipped) % value.length;
        int effectiveEndIdx = ((full ? (finishAt + 1) : 0) + Math.max(skipped, endIdx) - skipped) % value.length;
        if (effectiveBeginIdx >= effectiveEndIdx) {
            System.arraycopy(value, effectiveBeginIdx, resultArr, 0, length() - effectiveBeginIdx);
            System.arraycopy(value, 0, resultArr, length() - effectiveBeginIdx, effectiveEndIdx);
        }
        else
            System.arraycopy(value, effectiveBeginIdx, resultArr, 0, effectiveEndIdx - effectiveBeginIdx);
        return new String(resultArr);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        // Create a copy, don't share the array
        if (full) {
            char strVal[] = new char[value.length];
            int firstPart = value.length - finishAt - 1;

            System.arraycopy(value, finishAt + 1, strVal, 0, firstPart);
            System.arraycopy(value, 0, strVal, firstPart, value.length - firstPart);

            return new String(strVal, 0, strVal.length);
        }
        else
            return new String(value, 0, finishAt + 1);
    }

    /**
     * Performs an in-place rightward shift of elements within the circular buffer.
     * This is used to create space for new data by moving a block of existing elements.
     * <p>
     * The shift is executed in reverse order (from the end of the block to the beginning)
     * to prevent overwriting source elements before they are copied.
     *
     * @param shift The starting offset from the 'finishAt' index, defining the beginning
     *              of the block to be moved.
     * @param moveSteps The number of elements to be shifted to the right.
     */
    private void shiftRight(int shift, int moveSteps) {
        for (int i = 0; i < moveSteps; i++) {
            int pointer = (finishAt + shift - i) % value.length;
            value[pointer] = value[(value.length + pointer - shift) % value.length];
        }
    }

    /**
     * Performs a leftward shift of elements in the circular buffer.
     * Copies elements from a source position to a destination position,
     * effectively overwriting a range of values.
     * <p>
     * @param shift The offset for the source element.
     * @param shiftsCnt The count of elements to shift.
     */
    private void shiftLeft(int shift, int shiftsCnt) {
        for (int i = 0; i < shiftsCnt; i++) {
            int pointer = (finishAt + 1 + i) % value.length;
            value[pointer] = value[(pointer + shift) % value.length];
        }
    }

    /**
     * Inserts a substring from the source string into the buffer at the specified tail position.
     * <p>
     * The insertion is performed in reverse order (from the last character to the first) to
     * prevent overwriting source data in the buffer before it is copied. This is a common
     * technique for in-place buffer manipulation.
     *
     * @param src The source string to copy characters from.
     * @param tailEndOffset The physical index in the buffer where the last character will be placed.
     * @param insertCnt The number of characters from the source string to insert.
     */
    private void insertStringTail(String src, int tailEndOffset, int insertCnt) {
        for (int i = 0; i < insertCnt; i++)
            value[(value.length + tailEndOffset - i - 1) % value.length] = src.charAt(src.length() - 1 - i);
    }
}
