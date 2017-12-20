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

/**
 *
 */
public class SBLimitedLength extends SB {
    /** */
    private SBLengthLimit lenLimit;

    /**
     * @param cap Capacity.
     */
    SBLimitedLength(int cap) {
        super(cap);
    }

    /**
     * @return {@code True} if reached length limit.
     */
    boolean done() {
        return lenLimit.done();
    }

    /**
     * @param lenLimit Length limit.
     */
    void initLimit(SBLengthLimit lenLimit) {
        this.lenLimit = lenLimit;
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
        if (lenLimit.done())
            return this; 
            
        int curLen = length();

        super.a(obj);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(String str) {
        if (lenLimit.done())
            return this;

        int curLen = length();

        super.a(str);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(StringBuffer sb) {
        if (lenLimit.done())
            return this;

        int curLen = length();

        super.a(sb);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(CharSequence s) {
        if (lenLimit.done())
            return this;

        int curLen = length();

        super.a(s);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(CharSequence s, int start, int end) {
        if (lenLimit.done())
            return this;

        int curLen = length();

        super.a(s, start, end);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(char[] str) {
        if (lenLimit.done())
            return this;

        int curLen = length();

        super.a(str);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(char[] str, int offset, int len) {
        if (lenLimit.done())
            return this;

        int curLen = length();

        super.a(str, offset, len);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(boolean b) {
        if (lenLimit.done())
            return this;

        int curLen = length();

        super.a(b);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(char c) {
        if (lenLimit.done())
            return this;

        int curLen = length();

        super.a(c);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(int i) {
        if (lenLimit.done())
            return this;

        int curLen = length();

        super.a(i);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(long lng) {
        if (lenLimit.done())
            return this;

        int curLen = length();

        super.a(lng);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(float f) {
        if (lenLimit.done())
            return this;

        int curLen = length();

        super.a(f);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder a(double d) {
        if (lenLimit.done())
            return this;

        int curLen = length();

        super.a(d);

        return onWrite(curLen);
    }

    /** {@inheritDoc} */
    @Override public GridStringBuilder appendCodePoint(int codePoint) {
        if (lenLimit.done())
            return this;

        int curLen = length();

        super.appendCodePoint(codePoint);

        return onWrite(curLen);
    }

    /**
     * @param str String.
     */
    void appendNoLimitCheck(String str) {
        super.a(str);
    }
}
