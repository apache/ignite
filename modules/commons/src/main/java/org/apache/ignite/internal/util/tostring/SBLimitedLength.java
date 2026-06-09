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
import org.apache.ignite.internal.util.GridStringBuilder;

/**
 *
 */
public class SBLimitedLength extends GridStringBuilder {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private SBLengthLimit lenLimit;

    /** Additional string builder to get tail of message. */
    private CircularStringBuilder tail;

    /**
     * @param cap Capacity.
     */
    SBLimitedLength(int cap) {
        super(cap);
    }

    /**
     * @param lenLimit Length limit.
     */
    void initLimit(SBLengthLimit lenLimit) {
        this.lenLimit = lenLimit;

        if (tail != null)
            tail.reset();
    }

    /**
     * Resets buffer.
     */
    public void reset() {
        super.setLength(0);

        lenLimit.reset();

        if (tail != null)
            tail.reset();
    }

    /**
     * @return tail string builder.
     */
    public CircularStringBuilder getTail() {
        return tail;
    }

    /**
     * @param tail tail CircularStringBuilder to set.
     */
    public void setTail(CircularStringBuilder tail) {
        this.tail = tail;
    }

    /**
     * @param lenBeforeWrite Length before write.
     * @return This builder.
     */
    private GridStringBuilder onWrite(int lenBeforeWrite) {
        assert lenLimit != null;

        lenLimit.onWrite(this, length() - lenBeforeWrite);

        return this;
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(Object obj) {
        if (lenLimit.overflowed(this)) {
            tail.append(obj);
            return this;
        }

        int curLen = length();

        super.a(obj);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(String str) {
        if (lenLimit.overflowed(this)) {
            tail.append(str);
            return this;
        }

        int curLen = length();

        super.a(str);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(StringBuffer sb) {
        if (lenLimit.overflowed(this)) {
            tail.append(sb);
            return this;
        }

        int curLen = length();

        super.a(sb);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(CharSequence s) {
        if (lenLimit.overflowed(this)) {
            tail.append(s);
            return this;
        }

        int curLen = length();

        super.a(s);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(CharSequence s, int start, int end) {
        if (lenLimit.overflowed(this)) {
            tail.append(s.subSequence(start, end));
            return this;
        }

        int curLen = length();

        super.a(s, start, end);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(char[] str) {
        if (lenLimit.overflowed(this)) {
            tail.append(str);
            return this;
        }

        int curLen = length();

        super.a(str);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(char[] str, int offset, int len) {
        if (lenLimit.overflowed(this)) {
            tail.append(Arrays.copyOfRange(str, offset, len));
            return this;
        }

        int curLen = length();

        super.a(str, offset, len);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(boolean b) {
        if (lenLimit.overflowed(this)) {
            tail.append(b);
            return this;
        }

        int curLen = length();

        super.a(b);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(char c) {
        if (lenLimit.overflowed(this)) {
            tail.append(c);
            return this;
        }

        int curLen = length();

        super.a(c);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(int i) {
        if (lenLimit.overflowed(this)) {
            tail.append(i);
            return this;
        }

        int curLen = length();

        super.a(i);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(long lng) {
        if (lenLimit.overflowed(this)) {
            tail.append(lng);
            return this;
        }

        int curLen = length();

        super.a(lng);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(float f) {
        if (lenLimit.overflowed(this)) {
            tail.append(f);
            return this;
        }

        int curLen = length();

        super.a(f);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(double d) {
        if (lenLimit.overflowed(this)) {
            tail.append(d);
            return this;
        }

        int curLen = length();

        super.a(d);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder appendCodePoint(int codePoint) {
        if (lenLimit.overflowed(this)) {
            tail.append(codePoint);
            return this;
        }

        int curLen = length();

        super.appendCodePoint(codePoint);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        if (tail == null)
            return super.toString();
        else {
            int tailLen = tail.length();
            StringBuilder res = new StringBuilder(impl().length() + tailLen + 100);

            res.append(impl());

            if (tail.getSkipped() > 0) {
                res.append("... and ").append(String.valueOf(tail.getSkipped() + tailLen))
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
        return lenLimit.overflowed(this);
    }
}
