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
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.util.GridStringBuilder;

import static org.apache.ignite.internal.util.tostring.NodeRecursionMonitor.OBJECT_REGISTRY;

/**
 * The abstract base class for all nodes in the string representation tree.
 * Defines the common interface for appending a node's value to a string builder
 * and provides static factory and utility methods for all subclasses.
 */
public abstract class GridToStringNode {
    /**
     * A thread-local cache for nodes, used to handle references of
     * inner toString() calls by mapping temporary markers to actual nodes.
     */
    static final ConcurrentHashMap<Thread, IdentityHashMap<String, GridToStringNode>> CATCHED_NODES
            = new ConcurrentHashMap<>();

    /** Last constructed node. */
    static final ThreadLocal<GridToStringNode> LAST_CONSTRUCTED_GRID_TO_STRING_NODE = new ThreadLocal<>();

    /** The name of the property this node represents. */
    String propName;

    /** Inner buffer. For inner calls. */
    StringBuilder innerBuf = new StringBuilder(0);

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
     * Marks a node in the thread-local cache with a unique, empty string key.
     * Used to handle references during object graph traversal.
     * @param node The node to mark.
     * @return The unique marker string.
     */
    static String markNode(GridToStringNode node) {
        String result = new String(GridToStringNode.class.getSimpleName());
        identities()
                .orElseThrow()
                .put(result, node);
        return result;
    }

    /**
     * Appends inner buffer to last created node.
     * @param node Node - new node to fill inner buffer.
     */
    static String appendInnerBuffer(GridToStringNode node) {
        GridToStringNode parent = LAST_CONSTRUCTED_GRID_TO_STRING_NODE.get();
        parent.innerBuf.append(node.toString());
        return "";
    }

    /**
     * Checks if the current context is new, meaning no recursion or inner calls are in progress.
     * @return True if the context is new; false otherwise.
     */
    static boolean init() {
        Thread curThread = Thread.currentThread();
        boolean containsThread = CATCHED_NODES.containsKey(curThread);
        if (!containsThread)
            CATCHED_NODES.put(curThread, new IdentityHashMap<>());
        return !containsThread;
    }

    /**
     * Creates a node for a null value if the object is null.
     * @param obj The object to check.
     * @return An Optional containing a null node if the object is null; otherwise, an empty Optional.
     */
    static Optional<GridToStringNode> nullNode(Object obj) {
        return obj == null ? Optional.of(new GridToStringNullNode(null)) : Optional.empty();
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
     * Clears the thread-local cache of nodes and recursion prevention storage.
     */
    static void clear() {
        CATCHED_NODES.remove(Thread.currentThread());
        OBJECT_REGISTRY.remove();
        LAST_CONSTRUCTED_GRID_TO_STRING_NODE.remove();
    }

    /**
     * Generates the final string representation of this node.
     * Initializes a limited-length string builder, appends the node's content,
     * clears the cache, and returns the result.
     * @return The string representation of the node.
     */
    @Override public String toString() {
        SBLimitedLength sb = new SBLimitedLength(256);
        sb.initLimit(new SBLengthLimit());
        appendNode(sb);
        return sb.toString();
    }

    /** Retrieves identity hash map if it was initialized earlier
     * @return Optional identity hash map that stores catched nodes
     * */
    static Optional<IdentityHashMap<String, GridToStringNode>> identities() {
        return Optional.ofNullable(CATCHED_NODES.get(Thread.currentThread()));
    }

    /**
     * If someone decided to concat outside of default
     * {@link org.apache.ignite.internal.util.typedef.internal.S#toString}
     * we should replace the substring
     * @param obj Object.
     */
    public static <T> T recoverObject(T obj) {
        return Optional.ofNullable(obj)
                .filter(String.class::isInstance)
                .map(String.class::cast)
                .flatMap(str -> identities()
                        .map(storage -> storage.remove(str))
                        .map(GridToStringNode::toString)
                        .map(result -> (T)result))
                .orElse(obj);
    }
}
