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

package org.apache.ignite.internal.thread.context;

/** Represents entry point to manage Thread Context {@link Scope}s and associated attributes values. */
public class ThreadContext {
    /** */
    private static final ThreadLocal<ThreadContextData> THREAD_CONTEXT_DATA_HOLDER = ThreadLocal.withInitial(ThreadContextData::new);

    /** */
    private ThreadContext() {
        // No-op.
    }

    /**
     * @param attr Attribute.
     * @return Value bound to the specified attribute for the current scope.
     */
    public static <T> T get(ThreadContextAttribute<T> attr) {
        return data().get(attr);
    }

    /**
     * Creates {@link Scope} with no attribute values bound to it.
     * @return Created {@link Scope} instance.
     */
    public static Scope createScope() {
        return ThreadContextScope.create();
    }

    /**
     * Creates {@link Scope} with specified attribute value bound to it.
     *
     * @param attr Attribute.
     * @param val Attribute value.
     * @return Created {@link Scope} instance.
     */
    public static <T> Scope withAttribute(ThreadContextAttribute<T> attr, T val) {
        return createScope().withAttribute(attr, val);
    }

    /**
     * Creates {@link Scope} with attribute values restored from specified snapshot.
     *
     * @param snapshot Snapshot of thread Context attribute values.
     * @return Created {@link Scope} instance.
     */
    public static Scope withSnapshot(ThreadContextSnapshot snapshot) {
        return ThreadContextScope.createWith(snapshot);
    }

    /** @return Snapshot of thread Context attribute values. */
    public static ThreadContextSnapshot createSnapshot() {
        return data().createSnapshot();
    }

    /** */
    static ThreadContextData data() {
        return THREAD_CONTEXT_DATA_HOLDER.get();
    }
}
