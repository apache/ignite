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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Optimized string builder with better API.
 */
public class IgniteStringBuilder implements Serializable {
    /** System line separator. */
    private static final String NL = System.getProperty("line.separator");

    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Delegate. */
    private StringBuilder impl;

    /**
     * Creates string builder.
     *
     * @see StringBuilder#StringBuilder()
     */
    public IgniteStringBuilder() {
        impl = new StringBuilder(16);
    }

    /**
     * Creates string builder.
     *
     * @param cap Initial capacity.
     * @see StringBuilder#StringBuilder(int)
     */
    public IgniteStringBuilder(int cap) {
        impl = new StringBuilder(cap);
    }

    /**
     * Creates string builder.
     *
     * @param str Initial string.
     * @see StringBuilder#StringBuilder(String)
     */
    public IgniteStringBuilder(String str) {
        impl = new StringBuilder(str);
    }

    /**
     * Creates string builder.
     *
     * @param seq Initial character sequence.
     * @see StringBuilder#StringBuilder(CharSequence)
     */
    public IgniteStringBuilder(CharSequence seq) {
        impl = new StringBuilder(seq);
    }

    /**
     * Sets length of underlying character sequence.
     *
     * @param len Length to set.
     * @see StringBuilder#setLength(int)
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
     * Returns the length of underlying character sequence.
     *
     * @return Characters count.
     * @see StringBuilder#length()
     */
    public int length() {
        return impl.length();
    }

    /**
     * Appends the string representation of the {@code Object} argument.
     *
     * @param obj Element to add.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder a(Object obj) {
        impl.append(obj);

        return this;
    }

    /**
     * Appends the specified string to this character sequence.
     *
     * @param str Element to add.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder a(String str) {
        impl.append(str);

        return this;
    }

    /**
     * Appends the specified {@code StringBuffer} to this sequence.
     *
     * @param sb Element to add.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder a(StringBuffer sb) {
        impl.append(sb);

        return this;
    }

    /**
     * Appends a {@code CharSequence} to this sequence.
     *
     * @param s Element to add.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder a(CharSequence s) {
        impl.append(s);

        return this;
    }

    /**
     * Appends a subsequence of the specified {@code CharSequence} to this sequence.
     *
     * @param s Element to add.
     * @param start Start position.
     * @param end End position.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder a(CharSequence s, int start, int end) {
        impl.append(s, start, end);

        return this;
    }

    /**
     * Appends the string representation of the {@code char} array argument to this sequence.
     *
     * @param str Element to add.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder a(char[] str) {
        impl.append(str);

        return this;
    }

    /**
     * Appends the string representation of a subarray of the {@code char} array argument to this sequence.
     *
     * @param str Element to add.
     * @param offset Start offset.
     * @param len Length.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder a(char[] str, int offset, int len) {
        impl.append(str, offset, len);

        return this;
    }

    /**
     * Appends the string representation of the {@code boolean} argument to the sequence.
     *
     * @param b Element to add.
     * @return This buffer for chaining method calls.
     */
    @SuppressWarnings("BooleanParameter")
    public IgniteStringBuilder a(boolean b) {
        impl.append(b);

        return this;
    }

    /**
     * Appends the string representation of the {@code char} argument to this sequence.
     *
     * @param c Element to add.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder a(char c) {
        impl.append(c);

        return this;
    }

    /**
     * Appends the string representation of the {@code int} argument to this sequence.
     *
     * @param i Element to add.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder a(int i) {
        impl.append(i);

        return this;
    }

    /**
     * Appends the string representation of the {@code long} argument to this sequence.
     *
     * @param lng Element to add.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder a(long lng) {
        impl.append(lng);

        return this;
    }

    /**
     * Appends the string representation of the {@code float} argument to this sequence.
     *
     * @param f Element to add.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder a(float f) {
        impl.append(f);

        return this;
    }

    /**
     * Appends the string representation of the {@code double} argument to this sequence.
     *
     * @param d Element to add.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder a(double d) {
        impl.append(d);

        return this;
    }

    /**
     * Appends the string representation of the {@code codePoint} argument to this sequence.
     *
     * @param codePoint Element to add.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder appendCodePoint(int codePoint) {
        impl.appendCodePoint(codePoint);

        return this;
    }

    /**
     * Removes the characters in a substring of this sequence.
     *
     * @param start Start position to delete from.
     * @param end End position.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder d(int start, int end) {
        impl.delete(start, end);

        return this;
    }

    /**
     * Removes the {@code char} at the specified position in this sequence.
     *
     * @param index Index to delete character at.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder d(int index) {
        impl.deleteCharAt(index);

        return this;
    }

    /**
     * Adds a platform-dependent newline to this buffer.
     *
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder nl() {
        impl.append(NL);

        return this;
    }

    /**
     * Replaces the characters in a substring of this sequence with characters in the specified {@code String}.
     *
     * @param start Start position to replace from.
     * @param end End position.
     * @param str String to replace with.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder r(int start, int end, String str) {
        impl.replace(start, end, str);

        return this;
    }

    /**
     * Inserts the string representation of a subarray of the {@code str} array argument into this sequence.
     *
     * @param index Start index to insert to.
     * @param str String to insert.
     * @param off Offset in the string to be inserted.
     * @param len Length of the substring to be inserted.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder i(int index, char[] str, int off, int len) {
        impl.insert(index, str, off, len);

        return this;
    }

    /**
     * Inserts the string representation of the {@code Object} argument.
     *
     * @param off Offset to be inserted at.
     * @param obj Object whose string representation to be inserted.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder i(int off, Object obj) {
        return i(off, String.valueOf(obj));
    }

    /**
     * Inserts the string into this character sequence.
     *
     * @param off Offset to insert at.
     * @param str String to be inserted.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder i(int off, String str) {
        impl.insert(off, str);

        return this;
    }

    /**
     * Inserts the string representation of the {@code char} array argument into this sequence.
     *
     * @param off Offset to insert at.
     * @param str String to be inserted.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder i(int off, char[] str) {
        impl.insert(off, str);

        return this;
    }

    /**
     * Inserts the specified {@code CharSequence} into this sequence.
     *
     * @param dstOff Offset to insert at.
     * @param s String to insert.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder i(int dstOff, CharSequence s) {
        impl.insert(dstOff, s);

        return this;
    }

    /**
     * Inserts a subsequence of the specified {@code CharSequence} into this sequence.
     *
     * @param dstOff Offset to insert at.
     * @param s String to insert.
     * @param start Start index to insert from.
     * @param end End index to insert up to.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder i(int dstOff, CharSequence s, int start, int end) {
        impl.insert(dstOff, s, start, end);

        return this;
    }

    /**
     * Inserts the string representation of the {@code boolean} argument into this sequence.
     *
     * @param off Offset to insert at.
     * @param b Element to insert.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder i(int off, boolean b) {
        impl.insert(off, b);

        return this;
    }

    /**
     * Inserts the string representation of the {@code char} argument into this sequence.
     *
     * @param off Offset to insert at.
     * @param c Element to insert.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder i(int off, char c) {
        impl.insert(off, c);

        return this;
    }

    /**
     * Inserts the string representation of the {@code int} argument into this sequence.
     *
     * @param off Offset to insert at.
     * @param i Element to insert.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder i(int off, int i) {
        return i(off, String.valueOf(i));
    }

    /**
     * Inserts the string representation of the {@code long} argument into this sequence.
     *
     * @param off Offset to insert at.
     * @param l Element to insert.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder i(int off, long l) {
        return i(off, String.valueOf(l));
    }

    /**
     * Inserts the string representation of the {@code float} argument into this sequence.
     *
     * @param off Offset to insert at.
     * @param f Element to insert.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder i(int off, float f) {
        return i(off, String.valueOf(f));
    }

    /**
     * Inserts the string representation of the {@code double} argument into this sequence.
     *
     * @param off Offset to insert at.
     * @param d Element to insert.
     * @return This buffer for chaining method calls.
     */
    public IgniteStringBuilder i(int off, double d) {
        return i(off, String.valueOf(d));
    }

    /**
     * Appends given long value as a hex string to this string builder.
     *
     * @param val Value to append.
     * @return This builder for chaining method calls.
     */
    public IgniteStringBuilder appendHex(long val) {
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
    public IgniteStringBuilder appendHex(int val) {
        String hex = Integer.toHexString(val);

        int len = hex.length();

        for (int i = 0; i < 8 - len; i++)
            a('0');

        a(hex);

        return this;
    }

    /**
     * Save the state of the inderlying {@code StringBuilder} instance to a stream (that is, serialize it).
     *
     * @param s Stream to write to.
     * @throws IOException Thrown in case of any IO errors.
     */
    private void writeObject(ObjectOutputStream s) throws IOException {
        s.writeObject(impl);
    }

    /**
     * Restores the state of the underlying StringBuffer from a stream.
     *
     * @param s Stream to read from.
     * @throws IOException Thrown in case of any IO errors.
     * @throws ClassNotFoundException Thrown if read class cannot be found.
     */
    private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException {
        impl = (StringBuilder)s.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return impl.toString();
    }
}
