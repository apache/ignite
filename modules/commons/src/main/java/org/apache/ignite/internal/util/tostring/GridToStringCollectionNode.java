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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import org.apache.ignite.internal.util.GridStringBuilder;

import static org.apache.ignite.internal.util.tostring.GridToStringBuilder.COLLECTION_LIMIT;
import static org.apache.ignite.internal.util.tostring.GridToStringNodeFactory.getGridToStringNode;

/**
 * A node that represents a collection (e.g., List, Set) in the string representation.
 * It creates nodes for each element and formats the output with square brackets.
 */
class GridToStringCollectionNode extends NodeRecursionMonitor {
    /** A list of child nodes, each representing an element of the collection. */
    private final Collection<GridToStringNode> col;

    /** The simple class name of the collection being represented. */
    private final String colSimpleClsName;

    /** The rule for appending a hint about skipped elements if the collection is too large. */
    private final LongSequenceSkipRule skipRule;

    /**
     * Constructs a new collection node.
     * Iterates over the input collection, creates a node for each element,
     * and populates the internal list up to the collection size limit.
     * @param propName The property name.
     * @param col The collection to represent.
     */
    GridToStringCollectionNode(String propName, Collection<?> col) {
        super(propName, col);
        try {
            acquireRecursionMonitor();
            colSimpleClsName = col.getClass().getSimpleName();
            this.col = new ArrayList<>(Math.min(col.size(), COLLECTION_LIMIT));
            skipRule = new LongSequenceSkipRule(col::size);
            Iterator<?> iter = col.iterator();
            while (iter.hasNext() && this.col.size() != COLLECTION_LIMIT) {
                Object obj = iter.next();
                GridToStringNode node = getGridToStringNode(null, () -> obj, () -> obj.getClass());
                this.col.add(node);
            }
        }
        finally {
            releaseRecursionMonitor();
        }
    }

    /**
     * Appends the string representation of the collection to the builder.
     * The format is: CollectionClassName [element1, element2, ...].
     * Also appends a hint about skipped elements if necessary.
     * @param sb The string builder to append to.
     */
    @Override void appendNode(GridStringBuilder sb) {
        super.appendNode(sb);
        sb.a(colSimpleClsName).a(" [");
        Iterator<GridToStringNode> iter = col.iterator();
        while (iter.hasNext()) {
            GridToStringNode next = iter.next();
            next.appendNode(sb);
            if (iter.hasNext())
                sb.a(", ");
        }
        skipRule.appendSkippedCountHint(sb);
        sb.a(']');
    }
}
