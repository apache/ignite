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

package org.apache.ignite.plugin.extensions.communication;

import org.apache.ignite.internal.direct.*;

/**
 * TODO
 */
public final class MessageWriteState {
    public static MessageWriteState create(MessageFormatter formatter) {
        MessageWriter writer = formatter.writer();

        MessageWriteState state = new MessageWriteState(writer);

        // TODO: rework
        ((DirectMessageWriter)writer).state(state);

        return state;
    }

    private final MessageWriter writer;

    private final Stack stack;

    private MessageWriteState(MessageWriter writer) {
        this.writer = writer;

        stack = new Stack(-1);
    }

    public MessageWriter writer() {
        return writer;
    }

    public boolean isTypeWritten() {
        return stack.current() >= 0;
    }

    public void setTypeWritten() {
        assert stack.current() == -1;

        stack.incrementCurrent();
    }

    public int index() {
        return stack.current();
    }

    public void increment() {
        stack.incrementCurrent();
    }

    public void forward() {
        stack.push();
    }

    public void backward(boolean finished) {
        if (finished)
            stack.resetCurrent();

        stack.pop();
    }

    public void reset() {
        stack.reset();
    }

    private static class Stack {
        private final int[] arr = new int[10];

        private final int initVal;

        private int pos;

        private Stack(int initVal) {
            this.initVal = initVal;

            for (int i = 0; i < arr.length; i++)
                arr[i] = initVal;
        }

        int current() {
            return arr[pos];
        }

        void incrementCurrent() {
            arr[pos]++;
        }

        void resetCurrent() {
            arr[pos] = initVal;
        }

        void push() {
            pos++;
        }

        void pop() {
            pos--;
        }

        void reset() {
            assert pos == 0;

            resetCurrent();
        }
    }
}
