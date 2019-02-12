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

package org.apache.ignite.internal.direct.state;

import java.lang.reflect.Array;
import java.util.Arrays;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteOutClosure;

/**
 * Message state.
 */
public class DirectMessageState<T extends DirectMessageStateItem> {
    /** Initial array size. */
    private static final int INIT_SIZE = 10;

    /** Item factory. */
    @GridToStringExclude
    private final IgniteOutClosure<T> factory;

    /** Stack array. */
    private T[] stack;

    /** Current position. */
    private int pos;

    /**
     * @param cls State item type.
     * @param factory Item factory.
     */
    public DirectMessageState(Class<T> cls, IgniteOutClosure<T> factory) {
        this.factory = factory;

        stack = (T[])Array.newInstance(cls, INIT_SIZE);

        stack[0] = factory.apply();
    }

    /**
     * @return Current item.
     */
    public T item() {
        return stack[pos];
    }

    /**
     * Go forward.
     */
    public void forward() {
        pos++;

        if (pos == stack.length) {
            T[] stack0 = stack;

            stack = (T[])Array.newInstance(stack.getClass().getComponentType(), stack.length << 1);

            System.arraycopy(stack0, 0, stack, 0, stack0.length);
        }

        if (stack[pos] == null)
            stack[pos] = factory.apply();
    }

    /**
     * Go backward.
     *
     * @param reset Whether to reset current item.
     */
    public void backward(boolean reset) {
        if (reset)
            stack[pos].reset();

        pos--;
    }

    /**
     * Resets state.
     */
    public void reset() {
        assert pos == 0;

        stack[0].reset();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DirectMessageState.class, this, "stack", Arrays.toString(stack));
    }
}
