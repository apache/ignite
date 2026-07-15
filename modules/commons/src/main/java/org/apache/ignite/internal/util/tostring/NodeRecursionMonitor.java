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
import java.util.Optional;
import org.apache.ignite.internal.util.GridStringBuilder;

/**
 * Abstract base class for nodes that tracks object references
 * to prevent infinite recursion during string representation building.
 */
abstract class NodeRecursionMonitor extends GridToStringNode {
    /** Thread-local registry to track objects currently being processed. */
    static final ThreadLocal<IdentityHashMap<Object, NodeRecursionMonitor>> OBJECT_REGISTRY = new ThreadLocal<>();

    /** The object being monitored for recursive references. */
    private final Object obj;

    /** Flag indicating if the identity hash code should be appended to the output. */
    boolean hashIsRequired;

    /**
     * Constructor.
     * @param propName Name of the property.
     * @param obj Object to monitor for recursion.
     */
    NodeRecursionMonitor(String propName, Object obj) {
        super(propName);
        this.obj = obj;
    }

    /**
     * Registers the current node in the thread-local registry for the given object.
     */
    void acquireRecursionMonitor() {
        OBJECT_REGISTRY.get().put(obj, this);
    }

    /** Removes the current node from the thread-local registry. */
    void releaseRecursionMonitor() {
        OBJECT_REGISTRY.get().remove(obj);
    }

    /** Sets the flag to require appending the identity hash code of the object. */
    void setHashRequired() {
        hashIsRequired = true;
    }

    /**
     * Appends the identity hash code of the object to the string builder if required.
     * @param sb The string builder to append to.
     */
    void appendHashIfRequired(GridStringBuilder sb) {
        if (hashIsRequired && obj != null)
            sb.a(GridToStringBuilder.identity(obj));
    }

    /**
     * Finds a recursion monitor for a given object in the registry.
     * @param obj The object to find.
     * @return An optional containing the monitor if found.
     */
    static Optional<NodeRecursionMonitor> findRecursionMonitor(Object obj) {
        return Optional.of(OBJECT_REGISTRY)
                .map(ThreadLocal::get)
                .map(map -> map.get(obj));
    }
}
