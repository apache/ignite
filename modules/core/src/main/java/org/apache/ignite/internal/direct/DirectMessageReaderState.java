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

import org.apache.ignite.plugin.extensions.communication.MessageFactory;

/**
 * Writer state.
 */
public class DirectMessageReaderState {
    /** Initial array size. */
    private static final int INIT_SIZE = 10;

    /** Message factory. */
    private final MessageFactory msgFactory;

    /** Stack array. */
    private StateItem[] stack;

    /** Current position. */
    private int pos;

    /**
     * @param msgFactory Message factory.
     */
    public DirectMessageReaderState(MessageFactory msgFactory) {
        this.msgFactory = msgFactory;

        stack = new StateItem[INIT_SIZE];

        stack[0] = new StateItem(msgFactory);
    }

    /**
     * @return Current state.
     */
    public int state() {
        return stack[pos].state;
    }

    /**
     * Increments state.
     */
    public void incrementState() {
        stack[pos].state++;
    }

    /**
     * @return Current stream.
     */
    public DirectByteBufferStream stream() {
        return stack[pos].stream;
    }

    /**
     * Callback called before inner message is written.
     */
    public void beforeInnerMessageRead() {
        pos++;

        // Growing never happen for Ignite messages, but we need
        // to support it for custom messages from plugins.
        if (pos == stack.length) {
            StateItem[] stack0 = stack;

            stack = new StateItem[stack.length << 1];

            System.arraycopy(stack0, 0, stack, 0, stack0.length);
        }

        if (stack[pos] == null)
            stack[pos] = new StateItem(msgFactory);
    }

    /**
     * Callback called after inner message is written.
     *
     * @param finished Whether message was fully written.
     */
    public void afterInnerMessageRead(boolean finished) {
        if (finished)
            stack[pos].state = 0;

        pos--;
    }

    /**
     * Resets state.
     */
    public void reset() {
        assert pos == 0;

        stack[0].state = 0;
    }

    /**
     * State item.
     */
    private static class StateItem {
        /** Stream. */
        private final DirectByteBufferStream stream;

        /** State. */
        private int state;

        /**
         * @param msgFactory Message factory.
         */
        public StateItem(MessageFactory msgFactory) {
            stream = new DirectByteBufferStream(msgFactory);
        }
    }
}
