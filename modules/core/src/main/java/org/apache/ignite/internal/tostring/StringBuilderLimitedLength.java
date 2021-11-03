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

import static org.apache.ignite.lang.IgniteSystemProperties.IGNITE_TO_STRING_MAX_LENGTH;

import java.util.Arrays;
import org.apache.ignite.lang.IgniteStringBuilder;
import org.apache.ignite.lang.IgniteSystemProperties;
import org.jetbrains.annotations.Nullable;

/**
 * String builder with limited length.
 *
 * <p>Keeps head and tail for long strings that not fit to the limit cutting the middle of the string.
 */
class StringBuilderLimitedLength extends IgniteStringBuilder {
    /** Max string length. */
    private static final int MAX_STR_LEN = IgniteSystemProperties.getInteger(IGNITE_TO_STRING_MAX_LENGTH, 10_000);

    /** Length of tail part of message. */
    private static final int TAIL_LEN = MAX_STR_LEN / 10 * 2;

    /** Length of head part of message. */
    private static final int HEAD_LEN = MAX_STR_LEN - TAIL_LEN;

    /** Additional string builder to get tail of message. */
    private CircularStringBuilder tail;

    /**
     * Constructor.
     *
     * @param cap Capacity.
     */
    StringBuilderLimitedLength(int cap) {
        super(cap);

        tail = null;
    }

    /**
     * Resets buffer.
     */
    public void reset() {
        setLength(0);

        if (tail != null) {
            tail.reset();
        }
    }

    /**
     * Returns tail string builder.
     *
     * @return Tail string builder.
     */
    public @Nullable CircularStringBuilder getTail() {
        return tail;
    }

    /**
     * Callback to write a chars.
     *
     * @return This builder.
     */
    private StringBuilderLimitedLength onWrite() {
        if (!isOverflowed()) {
            return this;
        }

        if (tail == null) {
            tail = new CircularStringBuilder(TAIL_LEN);
        }

        if (tail.length() == 0) {
            int newSbLen = Math.min(length(), HEAD_LEN + 1);
            tail.append(impl().substring(newSbLen));

            setLength(newSbLen);
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public StringBuilderLimitedLength app(Object obj) {
        if (isOverflowed()) {
            tail.append(obj);
            return this;
        }

        super.app(obj);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override
    public StringBuilderLimitedLength app(String str) {
        if (isOverflowed()) {
            tail.append(str);
            return this;
        }

        super.app(str);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override
    public StringBuilderLimitedLength app(StringBuffer sb) {
        if (isOverflowed()) {
            tail.append(sb);
            return this;
        }

        super.app(sb);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override
    public StringBuilderLimitedLength app(CharSequence s) {
        if (isOverflowed()) {
            tail.append(s);
            return this;
        }

        super.app(s);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override
    public StringBuilderLimitedLength app(CharSequence s, int start, int end) {
        if (isOverflowed()) {
            tail.append(s.subSequence(start, end));
            return this;
        }

        super.app(s, start, end);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override
    public StringBuilderLimitedLength app(char[] str) {
        if (isOverflowed()) {
            tail.append(str);
            return this;
        }

        super.app(str);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override
    public StringBuilderLimitedLength app(char[] str, int offset, int len) {
        if (isOverflowed()) {
            tail.append(Arrays.copyOfRange(str, offset, len));
            return this;
        }

        super.app(str, offset, len);

        return onWrite();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("BooleanParameter")
    @Override
    public StringBuilderLimitedLength app(boolean b) {
        if (isOverflowed()) {
            tail.append(b);
            return this;
        }

        super.app(b);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override
    public StringBuilderLimitedLength app(char c) {
        if (isOverflowed()) {
            tail.append(c);
            return this;
        }

        super.app(c);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override
    public StringBuilderLimitedLength app(int i) {
        if (isOverflowed()) {
            tail.append(i);
            return this;
        }

        super.app(i);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override
    public StringBuilderLimitedLength app(long lng) {
        if (isOverflowed()) {
            tail.append(lng);
            return this;
        }

        super.app(lng);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override
    public StringBuilderLimitedLength app(float f) {
        if (isOverflowed()) {
            tail.append(f);
            return this;
        }

        super.app(f);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override
    public StringBuilderLimitedLength app(double d) {
        if (isOverflowed()) {
            tail.append(d);
            return this;
        }

        super.app(d);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override
    public StringBuilderLimitedLength appendCodePoint(int codePoint) {
        if (isOverflowed()) {
            tail.append(codePoint);
            return this;
        }

        super.appendCodePoint(codePoint);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        if (tail == null) {
            return super.toString();
        } else {
            int tailLen = tail.length();
            StringBuilder res = new StringBuilder(impl().capacity() + tailLen + 100);

            res.append(impl());

            if (tail.getSkipped() > 0) {
                res.append("... and ").append((tail.getSkipped() + tailLen))
                        .append(" skipped ...");
            }

            res.append(tail.toString());

            return res.toString();
        }
    }

    /**
     * Checks if the buffer is full.
     *
     * @return {@code True} - if buffer limit is reached.
     */
    public boolean isOverflowed() {
        return impl().length() > HEAD_LEN;
    }
}
