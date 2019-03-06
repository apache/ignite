/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
