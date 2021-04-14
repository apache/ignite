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
import org.apache.ignite.lang.IgniteStringBuilder;
import org.apache.ignite.lang.IgniteSystemProperties;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.lang.IgniteSystemProperties.IGNITE_TO_STRING_MAX_LENGTH;

/**
 * String builder with limited length.
 * <p>
 * Keeps head and tail for long strings that not fit to the limit cutting the middle of the string.
 */
class SBLimitedLength extends IgniteStringBuilder {
    /** Max string length. */
    private static final int MAX_STR_LEN = IgniteSystemProperties.getInteger(IGNITE_TO_STRING_MAX_LENGTH, 10_000);

    /** Length of tail part of message. */
    private static final int TAIL_LEN = MAX_STR_LEN / 10 * 2;

    /** Length of head part of message. */
    private static final int HEAD_LEN = MAX_STR_LEN - TAIL_LEN;

    /** Additional string builder to get tail of message. */
    private CircularStringBuilder tail;

    /**
     * @param cap Capacity.
     */
    SBLimitedLength(int cap) {
        super(cap);

        tail = null;
    }

    /**
     * Resets buffer.
     */
    public void reset() {
        setLength(0);

        if (tail != null)
            tail.reset();
    }

    /**
     * @return tail string builder.
     */
    public @Nullable CircularStringBuilder getTail() {
        return tail;
    }

    /**
     * @return This builder.
     */
    private SBLimitedLength onWrite() {
        if (!isOverflowed())
            return this;

        if (tail == null)
            tail = new CircularStringBuilder(TAIL_LEN);

        if (tail.length() == 0) {
            int newSbLen = Math.min(length(), HEAD_LEN + 1);
            tail.append(impl().substring(newSbLen));

            setLength(newSbLen);
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public SBLimitedLength a(Object obj) {
        if (isOverflowed()) {
            tail.append(obj);
            return this;
        }

        super.a(obj);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override public SBLimitedLength a(String str) {
        if (isOverflowed()) {
            tail.append(str);
            return this;
        }

        super.a(str);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override public SBLimitedLength a(StringBuffer sb) {
        if (isOverflowed()) {
            tail.append(sb);
            return this;
        }

        super.a(sb);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override public SBLimitedLength a(CharSequence s) {
        if (isOverflowed()) {
            tail.append(s);
            return this;
        }

        super.a(s);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override public SBLimitedLength a(CharSequence s, int start, int end) {
        if (isOverflowed()) {
            tail.append(s.subSequence(start, end));
            return this;
        }

        super.a(s, start, end);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override public SBLimitedLength a(char[] str) {
        if (isOverflowed()) {
            tail.append(str);
            return this;
        }

        super.a(str);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override public SBLimitedLength a(char[] str, int offset, int len) {
        if (isOverflowed()) {
            tail.append(Arrays.copyOfRange(str, offset, len));
            return this;
        }

        super.a(str, offset, len);

        return onWrite();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("BooleanParameter")
    @Override public SBLimitedLength a(boolean b) {
        if (isOverflowed()) {
            tail.append(b);
            return this;
        }

        super.a(b);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override public SBLimitedLength a(char c) {
        if (isOverflowed()) {
            tail.append(c);
            return this;
        }

        super.a(c);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override public SBLimitedLength a(int i) {
        if (isOverflowed()) {
            tail.append(i);
            return this;
        }

        super.a(i);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override public SBLimitedLength a(long lng) {
        if (isOverflowed()) {
            tail.append(lng);
            return this;
        }

        super.a(lng);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override public SBLimitedLength a(float f) {
        if (isOverflowed()) {
            tail.append(f);
            return this;
        }

        super.a(f);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override public SBLimitedLength a(double d) {
        if (isOverflowed()) {
            tail.append(d);
            return this;
        }

        super.a(d);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override public SBLimitedLength appendCodePoint(int codePoint) {
        if (isOverflowed()) {
            tail.append(codePoint);
            return this;
        }

        super.appendCodePoint(codePoint);

        return onWrite();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        if (tail == null)
            return super.toString();
        else {
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
     * @return {@code True} - if buffer limit is reached.
     */
    public boolean isOverflowed() {
        return impl().length() > HEAD_LEN;
    }
}
