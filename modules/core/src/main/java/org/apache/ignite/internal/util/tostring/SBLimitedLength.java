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

import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.internal.SB;
import sun.plugin2.jvm.CircularByteBuffer;

import java.util.ArrayDeque;
import java.util.Arrays;

/**
 *
 */
public class SBLimitedLength extends GridStringBuilder {
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
        tail = null;
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
        if (lenLimit.overflowed()) {
            tail.append(obj);
            return this;
        }

        int curLen = length();

        super.a(obj);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(String str) {
        if (lenLimit.overflowed()) {
            tail.append(str);
            return this;
        }

        int curLen = length();

        super.a(str);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(StringBuffer sb) {
        if (lenLimit.overflowed()) {
            tail.append(sb);
            return this;
        }

        int curLen = length();

        super.a(sb);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(CharSequence s) {
        if (lenLimit.overflowed()) {
            tail.append(s);
            return this;
        }

        int curLen = length();

        super.a(s);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(CharSequence s, int start, int end) {
        if (lenLimit.overflowed()) {
            tail.append(s.subSequence(start, end));
            return this;
        }

        int curLen = length();

        super.a(s, start, end);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(char[] str) {
        if (lenLimit.overflowed()) {
            tail.append(str);
            return this;
        }

        int curLen = length();

        super.a(str);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(char[] str, int offset, int len) {
        if (lenLimit.overflowed()) {
            tail.append(Arrays.copyOfRange(str, offset, len));
            return this;
        }

        int curLen = length();

        super.a(str, offset, len);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(boolean b) {
        if (lenLimit.overflowed()) {
            tail.append(b);
            return this;
        }

        int curLen = length();

        super.a(b);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(char c) {
        if (lenLimit.overflowed()) {
            tail.append(c);
            return this;
        }

        int curLen = length();

        super.a(c);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(int i) {
        if (lenLimit.overflowed()) {
            tail.append(i);
            return this;
        }

        int curLen = length();

        super.a(i);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(long lng) {
        if (lenLimit.overflowed()) {
            tail.append(lng);
            return this;
        }

        int curLen = length();

        super.a(lng);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(float f) {
        if (lenLimit.overflowed()) {
            tail.append(f);
            return this;
        }

        int curLen = length();

        super.a(f);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(double d) {
        if (lenLimit.overflowed()) {
            tail.append(d);
            return this;
        }

        int curLen = length();

        super.a(d);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder appendCodePoint(int codePoint) {
        if (lenLimit.overflowed()) {
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
            StringBuilder result = new StringBuilder(impl().capacity() + 100);
            result.append(impl());
            result.append("... and ").append(String.valueOf(tail.getSkipped() + tailLen))
                .append(" skipped ...").append(tail.toString());
            return result.toString();
        }
    }

    /**
     * @param str String.
     */
    void appendNoLimitCheck(String str) {
        super.a(str);
    }

}
