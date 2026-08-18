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
            if (tail == null)
                tail = lenLimit.createTail();
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
            if (tail == null)
                tail = lenLimit.createTail();
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
            if (tail == null)
                tail = lenLimit.createTail();
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
            if (tail == null)
                tail = lenLimit.createTail();
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
            if (tail == null)
                tail = lenLimit.createTail();
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
            if (tail == null)
                tail = lenLimit.createTail();
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
            if (tail == null)
                tail = lenLimit.createTail();
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
            if (tail == null)
                tail = lenLimit.createTail();
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
            if (tail == null)
                tail = lenLimit.createTail();
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
            if (tail == null)
                tail = lenLimit.createTail();
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
            if (tail == null)
                tail = lenLimit.createTail();
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
            if (tail == null)
                tail = lenLimit.createTail();
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
            if (tail == null)
                tail = lenLimit.createTail();
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
            if (tail == null)
                tail = lenLimit.createTail();
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

    /** {@inheritDoc} */
    @Override public GridStringBuilder i(int offset, String str) {
        int headLengthLimit = lenLimit.getHeadLengthLimit();
        if (offset < headLengthLimit) {
            impl().insert(offset, str);
            if (lenLimit.overflowed(this)) {
                String tailCandidate = impl().substring(headLengthLimit);
                if (tail == null)
                    tail = lenLimit.createTail();
                tail.insert(0, tailCandidate);
                impl().setLength(headLengthLimit);
            }
            return this;
        }
        // INVARIANT: tail is guaranteed to exist when offset >= headLengthLimit,
        // because overflow would have created tail before head could reach this offset.
        assert tail != null;
        tail.insert(offset - headLengthLimit, str);
        return this;
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder i(int idx, char[] str, int off, int len) {
        return i(idx, new String(str, off, len));
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder i(int off, Object obj) {
        return i(off, String.valueOf(obj));
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder i(int off, char[] str) {
        return i(off, new String(str));
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder i(int dstOff, CharSequence s) {
        return i(dstOff, s.toString());
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder i(int dstOff, CharSequence s, int start, int end) {
        return i(dstOff, s.subSequence(start, end).toString());
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder i(int off, boolean b) {
        return i(off, String.valueOf(b));
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder i(int off, char c) {
        return i(off, String.valueOf(c));
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder i(int off, int i) {
        return i(off, String.valueOf(i));
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder i(int off, long l) {
        return i(off, String.valueOf(l));
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder i(int off, float f) {
        return i(off, String.valueOf(f));
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder i(int off, double d) {
        return i(off, String.valueOf(d));
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder d(int start, int end) {
        throw new UnsupportedOperationException("Not supported by this implementation");
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder d(int idx) {
        throw new UnsupportedOperationException("Not supported by this implementation");
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder r(int start, int end, String str) {
        throw new UnsupportedOperationException("Not supported by this implementation");
    }
    
    /** {@inheritDoc} */
    @Override public GridStringBuilder nl() {
        return a(org.apache.ignite.internal.util.CommonUtils.nl());
    }

    /** {@inheritDoc} */
    @Override public int length() {
        int length = super.length();
        if (tail != null)
            length += tail.getSkipped() + tail.length();
        return length;
    }

    /** {@inheritDoc} */
    @Override public void setLength(int len) {
        throw new UnsupportedOperationException("setLength is not supported by this implementation");
    }
}
