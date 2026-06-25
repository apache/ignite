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

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.internal.util.GridStringBuilder;

import static org.apache.ignite.internal.util.tostring.GridToStringNodeFactory.getGridToStringNode;

/**
 * Represents a complex object node that contains and manages child property nodes.
 * It is responsible for building the string representation of a user-defined object,
 * including its class name and a list of its fields.
 */
class GridToStringObjectNode extends NodeRecursionMonitor {
    /** An unmodifiable list of child nodes representing the object's properties. */
    private final List<GridToStringNode> childNodes;

    /** The simple class name of the object being represented. */
    private final String entryName;

    /**
     * Constructor for a root node with a custom entry name and pre-defined child nodes.
     * This is typically used for the top-level object being stringified.
     * @param str The custom entry name to display.
     * @param childNodes The pre-constructed list of child nodes to include.
     */
    GridToStringObjectNode(String str, List<GridToStringNode> childNodes) {
        super(null, null);
        GridToStringNode parentObjNode = LAST_CONSTRUCTED_GRID_TO_STRING_NODE.get();
        LAST_CONSTRUCTED_GRID_TO_STRING_NODE.set(this);
        entryName = str;
        this.childNodes = Collections.unmodifiableList(childNodes);
        LAST_CONSTRUCTED_GRID_TO_STRING_NODE.set(parentObjNode);
    }

    /**
     * Constructor that builds a node for an object by reflecting its fields.
     * This constructor acquires a recursion monitor, retrieves field descriptors,
     * creates child nodes for each field, and then releases the monitor.
     * @param propName The property name of this object in the parent structure.
     * @param obj The object to represent.
     * @param cls The class of the object.
     * @param additionalChildNodes Any additional nodes to include in the output.
     */
    GridToStringObjectNode(String propName, Object obj, Class<?> cls, List<GridToStringNode> additionalChildNodes) {
        super(propName, obj);
        GridToStringNode parentObjNode = LAST_CONSTRUCTED_GRID_TO_STRING_NODE.get();
        LAST_CONSTRUCTED_GRID_TO_STRING_NODE.set(this);
        try {
            aqcuireRecursionMonitor(this);
            List<GridToStringNode> childNodes = new LinkedList<>();
            GridToStringClassDescriptor cd = GridToStringBuilder.getClassDescriptor(cls);
            for (GridToStringFieldDescriptor fd : cd.getFields()) {
                GridToStringNode childNode = getGridToStringNode(obj, fd);
                childNodes.add(childNode);
            }
            entryName = cd.getSimpleClassName();
            childNodes.addAll(additionalChildNodes);
            this.childNodes = Collections.unmodifiableList(childNodes);
        }
        finally {
            releaseRecursionMonitor();
            LAST_CONSTRUCTED_GRID_TO_STRING_NODE.set(parentObjNode);
        }
    }

    /**
     * Appends the object's class name and its property nodes to the string builder.
     * The output format is: [ClassName@hashCode] [field1=value1, field2=value2].
     * If a hash code is required (due to recursion), it is appended to the class name.
     * @param sb The string builder to append to.
     */
    @Override void appendNode(GridStringBuilder sb) {
        super.appendNode(sb);
        if (entryName != null) {
            sb.a(entryName);
            appendHashIfRequired(sb);
            sb.a(' ');
        }
        sb.a('[');
        sb.a(innerBuf);
        Iterator<GridToStringNode> iter = childNodes.iterator();
        while (iter.hasNext()) {
            GridToStringNode node = iter.next();
            node.appendNode(sb);
            if (iter.hasNext())
                sb.a(", ");
        }
        sb.a("]");
    }
}
