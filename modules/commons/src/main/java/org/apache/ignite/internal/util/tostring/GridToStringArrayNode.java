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

import java.lang.reflect.Array;
import org.apache.ignite.internal.util.GridStringBuilder;

import static org.apache.ignite.internal.util.tostring.GridToStringBuilder.COLLECTION_LIMIT;
import static org.apache.ignite.internal.util.tostring.GridToStringNodeFactory.getGridToStringNode;

/**
 * A node that represents an array in the string representation.
 * It creates nodes for each element of the array and formats the output with square brackets.
 */
class GridToStringArrayNode extends NodeRecursionMonitor {
    /** An array of child nodes, each representing an element of the source array. */
    private final GridToStringNode[] nodes;

    /** The rule for appending a hint about skipped elements if the array is too large. */
    private final LongSequenceSkipRule skipRule;

    /** The class object representing the type of the array. */
    private final Class<?> arrType;

    /**
     * Constructs a new array node.
     * Iterates over the input array, creates a node for each element,
     * and populates the internal array up to the collection size limit.
     * @param propName The property name.
     * @param arr The source array.
     * @param arrType The class object of the array's type.
     */
    GridToStringArrayNode(String propName, Object arr, Class<?> arrType) {
        super(propName, arr);
        try {
            acquireRecursionMonitor();
            this.arrType = arrType;
            int srcArrLength = Array.getLength(arr);
            skipRule = new LongSequenceSkipRule(() -> srcArrLength);
            nodes = new GridToStringNode[Math.min(COLLECTION_LIMIT, srcArrLength)];
            for (int i = 0; i < nodes.length; i++) {
                Object child = Array.get(arr, i);
                nodes[i] = getGridToStringNode(null, () -> child, child::getClass);
            }
        }
        finally {
            releaseRecursionMonitor();
        }
    }

    /**
     * Appends the string representation of the array to the builder.
     * The format is: ArrayType [element1, element2, ...].
     * Also appends a hint about skipped elements if necessary.
     * @param sb The string builder to append to.
     */
    @Override void appendNode(GridStringBuilder sb) {
        super.appendNode(sb);
        sb.a(arrType.getSimpleName()).a(" [");
        for (int i = 0; i < nodes.length - 1; i++) {
            nodes[i].appendNode(sb);
            sb.a(", ");
        }
        if (nodes.length > 0)
            nodes[nodes.length - 1].appendNode(sb);
        skipRule.appendSkippedCountHint(sb);
        sb.a("]");
    }
}
