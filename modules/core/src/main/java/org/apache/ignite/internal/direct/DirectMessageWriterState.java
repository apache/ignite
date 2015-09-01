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

package org.apache.ignite.internal.direct;

import java.util.Arrays;

/**
 * Writer state.
 */
public class DirectMessageWriterState {
    /** Initial array size. */
    private static final int INIT_SIZE = 10;

    /** Initial value. */
    private static final int INIT_VAL = -1;

    /** Stack array. */
    private int[] stack;

    /** Current position. */
    private int pos;

    /**
     *
     */
    public DirectMessageWriterState() {
        stack = new int[INIT_SIZE];

        Arrays.fill(stack, INIT_VAL);
    }

    /**
     * @return Whether type is written.
     */
    public boolean isTypeWritten() {
        return stack[pos] >= 0;
    }

    /**
     * Callback called after type is written.
     */
    public void onTypeWritten() {
        assert stack[pos] == -1;

        stack[pos] = 0;
    }

    /**
     * @return Current state.
     */
    public int state() {
        return stack[pos];
    }

    /**
     * Increments state.
     */
    public void incrementState() {
        stack[pos]++;
    }

    /**
     * @param val New state value.
     */
    protected void setState(int val) {
        stack[pos] = val;
    }

    /**
     * Callback called before inner message is written.
     */
    public void beforeInnerMessageWrite() {
        pos++;

        // Growing never happen for Ignite messages, but we need
        // to support it for custom messages from plugins.
        if (pos == stack.length) {
            int[] stack0 = stack;

            stack = new int[stack.length << 1];

            System.arraycopy(stack0, 0, stack, 0, stack0.length);

            Arrays.fill(stack, stack0.length, stack.length, INIT_VAL);
        }
    }

    /**
     * Callback called after inner message is written.
     *
     * @param finished Whether message was fully written.
     */
    public void afterInnerMessageWrite(boolean finished) {
        if (finished)
            stack[pos] = INIT_VAL;

        pos--;
    }

    /**
     * Resets state.
     */
    public void reset() {
        assert pos == 0;

        stack[0] = INIT_VAL;
    }
}