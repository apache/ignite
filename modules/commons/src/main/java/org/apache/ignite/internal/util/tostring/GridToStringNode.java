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

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import org.apache.ignite.internal.util.GridStringBuilder;

import static org.apache.ignite.internal.util.tostring.NodeRecursionMonitor.OBJECT_REGISTRY;

/**
 * The abstract base class for all nodes in the string representation tree.
 * Defines the common interface for appending a node's value to a string builder
 * and provides static factory and utility methods for all subclasses.
 */
abstract class GridToStringNode {
    /** Inner builder for calls that return empty string by design */
    static final ThreadLocal<StringBuilder> INNER_BUILDER = new ThreadLocal<>();

    /** The name of the property this node represents. */
    String propName;

    /** Inner buffer. For inner calls. */
    StringBuilder innerBuf;

    /** Previously calculated result. */
    String previouslyCalculatedResult;

    /**
     * Base constructor.
     * @param propName The name of the property.
     */
    GridToStringNode(String propName) {
        this.propName = propName;
    }

    /**
     * Creates a root node for a string value.
     * @param str The string value.
     * @param addNodes Additional child nodes to include.
     * @return A new root node.
     */
    static GridToStringNode getRootNode(String str, List<GridToStringNode> addNodes) {
        return new GridToStringObjectNode(str, addNodes);
    }

    /**
     * Creates a root node for an object.
     * Checks for recursion and creates either a termination node or a full object node.
     * @param obj The object to represent.
     * @param cls The class of the object.
     * @param addNodes Additional child nodes to include.
     * @return A new root node.
     */
    static GridToStringNode getRootNode(Object obj, Class<?> cls, List<GridToStringNode> addNodes) {
        return recursionTermination(obj)
                .orElseGet(() -> new GridToStringObjectNode(null, obj, cls, addNodes));
    }

    /**
     * Save node string representation to inner builder
     * @param node Node - new node to fill inner buffer.
     */
    static String memorizeNode(GridToStringNode node) {
        String result = node.toString();
        if (INNER_BUILDER.get() == null)
            INNER_BUILDER.set(new StringBuilder(result));
        else
            INNER_BUILDER.get().append(result);
        return "";
    }

    /**
     * Checks if the current context is new, meaning no recursion or inner calls are in progress.
     * @return True if the context is new; false otherwise.
     */
    static boolean init() {
        boolean isNew = OBJECT_REGISTRY.get() == null;
        if (isNew)
            OBJECT_REGISTRY.set(new IdentityHashMap<>());
        return isNew;
    }

    /**
     * Checks if an object is part of a recursive reference and, if so,
     * creates a termination node to break the cycle.
     * @param obj The object to check.
     * @return An Optional containing a termination node if recursion is detected;
     * otherwise, an empty Optional.
     */
    static Optional<GridToStringNode> recursionTermination(Object obj) {
        return Optional.ofNullable(obj)
                .flatMap(NodeRecursionMonitor::findRecursionMonitor)
                .map(sameObjNode ->
                        GridToStringRecursionTerminationNode.of(sameObjNode, obj));
    }

    /**
     * Appends the property name (if any) and a placeholder for the value to the builder.
     * This method is intended to be overridden by subclasses to provide specific value output.
     * @param sb The string builder to append to.
     */
    void appendNode(GridStringBuilder sb) {
        if (propName != null)
            sb.a(propName).a("=");
    }

    /**
     * Clears recursion prevention storage and inner calls string builder.
     */
    static void clear() {
        OBJECT_REGISTRY.remove();
        INNER_BUILDER.remove();
    }

    /**
     * Generates the final string representation of this node.
     * Initializes a limited-length string builder, appends the node's content,
     * clears the cache, and returns the result.
     * @return The string representation of the node.
     */
    @Override public String toString() {
        if (previouslyCalculatedResult != null)
            return previouslyCalculatedResult;
        SBLimitedLength sb = new SBLimitedLength(256);
        sb.initLimit(new SBLengthLimit());
        appendNode(sb);
        return previouslyCalculatedResult = sb.toString();
    }

    /**
     * Appends inner buffer if it exists to specified builder
     * @param sb Sb. string builder to append
     */
    void appendInnerBuffer(GridStringBuilder sb) {
        if (innerBuf != null)
            sb.a(innerBuf);
    }
}
