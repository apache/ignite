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

package org.apache.ignite.internal.pagemem.impl;

import java.util.NoSuchElementException;

/**
 *
 */
public class LongStack {
    /** */
    private static final int SIZE = 512;

    /** Stack elements. */
    private long[] els;

    /** */
    private int idx;

    /** */
    private LongStack prev;

    /**
     *
     */
    public LongStack() {
        els = new long[SIZE];
    }

    /**
     * Self-linking constructor.
     *
     * @param els Elements to save.
     */
    private LongStack(long[] els) {
        this.els = els;
        idx = els.length - 1;
    }

    /**
     * @param val Value to push.
     */
    public void push(long val) {
        if (idx == els.length - 1) {
            prev = new LongStack(els);

            els = new long[SIZE];
            idx = 0;
        }

        els[idx++] = val;
    }

    /**
     * @return Popped value.
     * @throws NoSuchElementException If stack is empty.
     */
    public long pop() throws NoSuchElementException {
        if (isEmpty())
            throw new NoSuchElementException();

        long ret = els[idx--];

        if (idx == 0 && prev != null) {
            els = prev.els;
            idx = els.length - 1;

            prev = prev.prev;
        }

        return ret;
    }

    /**
     * @return {@code True} if stack is empty.
     */
    public boolean isEmpty() {
        return idx == 0;
    }
 }
