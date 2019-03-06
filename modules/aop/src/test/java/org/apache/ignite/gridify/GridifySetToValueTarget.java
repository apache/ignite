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

package org.apache.ignite.gridify;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.compute.gridify.GridifySetToValue;

/**
 * Test set-to-value target.
 */
public class GridifySetToValueTarget implements GridifySetToValueTargetInterface, Serializable {
    /**
     * Find maximum value in collection.
     *
     * @param input Input collection.
     * @return Maximum value.
     */
    @GridifySetToValue(igniteInstanceName = "GridifySetToValueTarget", threshold = 2, splitSize = 2)
    @Override public Long findMaximum(Collection<Long> input) {
        return findMaximum0(input);
    }

    /**
     * Find maximum value in collection.
     *
     * @param input Input collection.
     * @return Maximum value.
     */
    @GridifySetToValue(igniteInstanceName = "GridifySetToValueTarget")
    @Override public Long findMaximumInList(List<Long> input) {
        return findMaximum0(input);
    }

    /**
     * Find maximum value in collection.
     *
     * @param input Input collection.
     * @return Maximum value.
     */
    @GridifySetToValue(igniteInstanceName = "GridifySetToValueTarget", threshold = 2)
    @Override public Long findMaximumWithoutSplitSize(Collection<Long> input) {
        return findMaximum0(input);
    }

    /**
     * Find maximum value in collection.
     *
     * @param input Input collection.
     * @return Maximum value.
     */
    @GridifySetToValue(igniteInstanceName = "GridifySetToValueTarget")
    @Override public Long findMaximumWithoutSplitSizeAndThreshold(Collection<Long> input) {
        return findMaximum0(input);
    }

    /**
     * Find maximum in array.
     *
     * @param input Input collection.
     * @return Maximum value.
     */
    @GridifySetToValue(igniteInstanceName = "GridifySetToValueTarget", threshold = 2, splitSize = 2)
    @Override public Long findPrimesInArray(Long[] input) {
        return findMaximumInArray0(input);
    }

    /**
     * Find maximum in primitive array.
     *
     * @param input Input collection.
     * @return Maximum value.
     */
    @GridifySetToValue(igniteInstanceName = "GridifySetToValueTarget", threshold = 2, splitSize = 2)
    @Override public long findMaximumInPrimitiveArray(long[] input) {
        return findMaximumInPrimitiveArray0(input);
    }

    /**
     * Find maximum value in collection.
     *
     * @param input Input collection.
     * @return Maximum value.
     */
    private Long findMaximum0(Collection<Long> input) {
        System.out.println(">>>");
        System.out.println("Find maximum in: " + input);
        System.out.println(">>>");

        return Collections.max(input);
    }

    /**
     * Find maximum value in array.
     *
     * @param input Input collection.
     * @return Maximum value.
     */
    private Long findMaximumInArray0(Long[] input) {
        System.out.println(">>>");
        System.out.println("Find maximum in array: " + Arrays.asList(input));
        System.out.println(">>>");

        return Collections.max(Arrays.asList(input));
    }

    /**
     * Find maximum value in array.
     *
     * @param input Input collection.
     * @return Maximum value.
     */
    private long findMaximumInPrimitiveArray0(long[] input) {
        assert input != null;
        assert input.length > 0;
        System.out.println(">>>");
        System.out.println("Find maximum in primitive array: " + Arrays.toString(input));
        System.out.println(">>>");

        long maximum = input[0];

        for (int i = 1; i < input.length; i++) {
            if (input[i] > maximum)
                maximum = input[i];
        }

        return maximum;
    }

    /**
     * Find maximum value in Iterator.
     *
     * @param input Input collection.
     * @return Maximum value.
     */
    @GridifySetToValue(igniteInstanceName = "GridifySetToValueTarget", threshold = 2, splitSize = 2)
    @Override public long findMaximumInIterator(Iterator<Long> input) {
        assert input != null;
        assert input.hasNext();

        System.out.println(">>>");
        System.out.println("Find maximum in iterator: " + input);
        System.out.println(">>>");

        long maximum = input.next();

        while(input.hasNext()) {
            Long val = input.next();

            if (val > maximum)
                maximum = val;
        }

        return maximum;
    }

    /**
     * Find maximum value in Enumeration.
     *
     * @param input Input collection.
     * @return Maximum value.
     */
    @GridifySetToValue(igniteInstanceName = "GridifySetToValueTarget", threshold = 2, splitSize = 2)
    @Override public long findMaximumInEnumeration(Enumeration<Long> input) {
        assert input != null;
        assert input.hasMoreElements();

        System.out.println(">>>");
        System.out.println("Find maximum in enumeration: " + input);
        System.out.println(">>>");

        long maximum = input.nextElement();

        while(input.hasMoreElements()) {
            Long val = input.nextElement();

            if (val > maximum)
                maximum = val;
        }

        return maximum;
    }
}