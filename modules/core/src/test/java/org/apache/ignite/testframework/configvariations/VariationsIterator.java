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

package org.apache.ignite.testframework.configvariations;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Variations iterator.
 */
public class VariationsIterator implements Iterator<int[]> {
    /** */
    private final Object[][] params;

    /** */
    private final int[] vector;

    /** */
    private int position;

    /** */
    private final int expCntOfVectors;

    /** */
    private int cntOfVectors;

    /**
     * @param params Paramethers.
     */
    public VariationsIterator(Object[][] params) {
        assert params != null;
        assert params.length > 0;

        for (int i = 0; i < params.length; i++) {
            assert params[i] != null : i;
            assert params[i].length > 0 : i;
        }

        this.params = params;

        vector = new int[params.length];

        for (int i = 0; i < vector.length; i++)
            vector[i] = 0;

        position = -1;

        int cntOfVectors0 = 1;

        for (int i = 0; i < params.length; i++)
            cntOfVectors0 *= params[i].length;

        expCntOfVectors = cntOfVectors0;

        cntOfVectors = 0;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return cntOfVectors < expCntOfVectors;
    }

    /** {@inheritDoc} */
    @Override public int[] next() {
        // Only first call.
        if (position == -1) {
            position = 0;

            cntOfVectors++;

            return arraycopy(vector);
        }

        if (!updateVector(vector, position)) {
            if (position + 1 == params.length)
                throw new IllegalStateException("[position=" + position + ", vector=" +
                    Arrays.toString(vector) + ", params=" + Arrays.deepToString(params));

            position++;

            // Skip params with length 1. We cannot set 1 at this position.
            while (position < params.length && params[position].length < 2)
                position++;

            if (position == params.length)
                throw new IllegalStateException("[position=" + position + ", vector=" +
                    Arrays.toString(vector) + ", params=" + Arrays.deepToString(params));

            vector[position] = 1;

            cntOfVectors++;

            return arraycopy(vector);
        }

        cntOfVectors++;

        return arraycopy(vector);
    }

    /**
     * Updates vector starting from position.
     *
     * @param vector Vector.
     * @param position Position.
     * @return {@code True} if vector has been updated. When {@code false} is returned it means that all positions
     *          before has been set to {@code 0}.
     */
    private boolean updateVector(int[] vector, int position) {
        if (position == 0) {
            int val = vector[0];

            if (val + 1 < params[0].length) {
                vector[0] = val + 1;

                return true;
            }
            else {
                vector[0] = 0;

                return false;
            }
        }

        if (updateVector(vector, position - 1))
            return true;

        int val = vector[position];

        if (val + 1 < params[position].length) {
            vector[position] = val + 1;

            return true;
        }
        else {
            vector[position] = 0;

            return false;
        }

    }

    /**
     * @param arr Array.
     * @return Array copy.
     */
    private static int[] arraycopy(int[] arr) {
        int[] dest = new int[arr.length];

        System.arraycopy(arr, 0, dest, 0, arr.length);

        return dest;
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        throw new UnsupportedOperationException();
    }
}
