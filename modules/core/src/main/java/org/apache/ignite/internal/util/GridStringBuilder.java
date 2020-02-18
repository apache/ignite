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

package org.apache.ignite.internal.util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Optimized string builder with better API.
 */
public class GridStringBuilder implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private StringBuilder impl;

    /**
     * @see StringBuilder#StringBuilder()
     */
    public GridStringBuilder() {
        impl = new StringBuilder(16);
    }

    /**
     *
     * @param cap Initial capacity.
     * @see StringBuilder#StringBuilder(int)
     */
    public GridStringBuilder(int cap) {
        impl = new StringBuilder(cap);
    }

    /**
     *
     * @param str Initial string.
     * @see StringBuilder#StringBuilder(String)
     */
    public GridStringBuilder(String str) {
        impl = new StringBuilder(str);
    }

    /**
     * @param seq Initial character sequence.
     * @see StringBuilder#StringBuilder(CharSequence)
     */
    public GridStringBuilder(CharSequence seq) {
        impl = new StringBuilder(seq);
    }

    /**
     *
     * @param len Length to set.
     */
    public void setLength(int len) {
        impl.setLength(len);
    }

    /**
     * Gets underlying implementation.
     *
     * @return Underlying implementation.
     */
    public StringBuilder impl() {
        assert impl != null;

        return impl;
    }

    /**
     *
     * @return Buffer length.
     */
    public int length() {
        return impl.length();
    }

    /**
     *
     * @param obj Element to add.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder a(Object obj) {
        impl.append(String.valueOf(obj));

        return this;
    }

    /**
     *
     * @param str Element to add.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder a(String str) {
        impl.append(str);

        return this;
    }

    /**
     *
     * @param sb Element to add.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder a(StringBuffer sb) {
        impl.append(sb);

        return this;
    }

    /**
     *
     * @param s Element to add.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder a(CharSequence s) {
        impl.append(s);

        return this;
    }

    /**
     *
     * @param s Element to add.
     * @param start Start position.
     * @param end End position.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder a(CharSequence s, int start, int end) {
        impl.append(s, start, end);

        return this;
    }

    /**
     *
     * @param str Element to add.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder a(char str[]) {
        impl.append(str);

        return this;
    }

    /**
     *
     * @param str Element to add.
     * @param offset Start offset.
     * @param len Length.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder a(char str[], int offset, int len) {
        impl.append(str, offset, len);

        return this;
    }

    /**
     *
     * @param b Element to add.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder a(boolean b) {
        impl.append(b);

        return this;
    }

    /**
     *
     * @param c Element to add.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder a(char c) {
        impl.append(c);

        return this;
    }

    /**
     *
     * @param i Element to add.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder a(int i) {
        impl.append(i);

        return this;
    }

    /**
     *
     * @param lng Element to add.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder a(long lng) {
        impl.append(lng);

        return this;
    }

    /**
     *
     * @param f Element to add.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder a(float f) {
        impl.append(f);

        return this;
    }

    /**
     *
     * @param d Element to add.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder a(double d) {
        impl.append(d);

        return this;
    }

    /**
     *
     * @param codePoint Element to add.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder appendCodePoint(int codePoint) {
        impl.appendCodePoint(codePoint);

        return this;
    }

    /**
     *
     * @param start Start position to delete from.
     * @param end End position.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder d(int start, int end) {
        impl.delete(start, end);

        return this;
    }

    /**
     *
     * @param index Index to delete character at.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder d(int index) {
        impl.deleteCharAt(index);

        return this;
    }

    /**
     * Adds a platform-dependent newline to this buffer.
     *
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder nl() {
        impl.append(U.nl());

        return this;
    }

    /**
     *
     * @param start Start position to replace from.
     * @param end End position.
     * @param str String to replace with.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder r(int start, int end, String str) {
        impl.replace(start, end, str);

        return this;
    }

    /**
     *
     * @param index Start index to insert to.
     * @param str String to insert.
     * @param off Offset in the string to be inserted.
     * @param len Length of the substring to be inserted.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder i(int index, char str[], int off, int len) {
        impl.insert(index, str, off, len);

        return this;
    }

    /**
     *
     * @param off Offset to be inserted at.
     * @param obj Object whose string representation to be inserted.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder i(int off, Object obj) {
        return i(off, String.valueOf(obj));
    }

    /**
     *
     * @param off Offset to insert at.
     * @param str String to be inserted.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder i(int off, String str) {
        impl.insert(off, str);

        return this;
    }

    /**
     *
     * @param off Offset to insert at.
     * @param str String to be inserted.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder i(int off, char str[]) {
        impl.insert(off, str);

        return this;
    }

    /**
     *
     * @param dstOff Offset to insert at.
     * @param s String to insert.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder i(int dstOff, CharSequence s) {
        impl.insert(dstOff, s);

        return this;
    }

    /**
     *
     * @param dstOff Offset to insert at.
     * @param s String to insert.
     * @param start Start index to insert from.
     * @param end End index to insert up to.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder i(int dstOff, CharSequence s, int start, int end) {
        impl.insert(dstOff, s, start, end);

        return this;
    }

    /**
     *
     * @param off Offset to insert at.
     * @param b Element to insert.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder i(int off, boolean b) {
        impl.insert(off, b);

        return this;
    }

    /**
     *
     * @param off Offset to insert at.
     * @param c Element to insert.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder i(int off, char c) {
        impl.insert(off, c);

        return this;
    }

    /**
     *
     * @param off Offset to insert at.
     * @param i Element to insert.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder i(int off, int i) {
        return i(off, String.valueOf(i));
    }

    /**
     *
     * @param off Offset to insert at.
     * @param l Element to insert.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder i(int off, long l) {
        return i(off, String.valueOf(l));
    }

    /**
     *
     * @param off Offset to insert at.
     * @param f Element to insert.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder i(int off, float f) {
        return i(off, String.valueOf(f));
    }

    /**
     *
     * @param off Offset to insert at.
     * @param d Element to insert.
     * @return This buffer for chaining method calls.
     */
    public GridStringBuilder i(int off, double d) {
        return i(off, String.valueOf(d));
    }

    /**
     * Appends given long value as a hex string to this string builder.
     *
     * @param val Value to append.
     * @return This builder for chaining method calls.
     */
    public GridStringBuilder appendHex(long val) {
        String hex = Long.toHexString(val);

        int len = hex.length();

        for (int i = 0; i < 16 - len; i++)
            a('0');

        a(hex);

        return this;
    }

    /**
     * Appends given long value as a hex string to this string builder.
     *
     * @param val Value to append.
     * @return This builder for chaining method calls.
     */
    public GridStringBuilder appendHex(int val) {
        String hex = Integer.toHexString(val);

        int len = hex.length();

        for (int i = 0; i < 8 - len; i++)
            a('0');

        a(hex);

        return this;
    }

    /**
     *
     * @param s Stream to write to.
     * @throws IOException Thrown in case of any IO errors.
     */
    private void writeObject(ObjectOutputStream s) throws IOException {
        s.writeObject(impl);
    }

    /**
     *
     * @param s Stream to read from.
     * @throws IOException Thrown in case of any IO errors.
     * @throws ClassNotFoundException Thrown if read class cannot be found.
     */
    private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException   {
        impl= (StringBuilder) s.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return impl.toString();
    }
}
