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

/** */
public class ThreadContext {
    /** */
    private static final ThreadLocal<ThreadContextData> THREAD_CONTEXT_DATA_HOLDER = ThreadLocal.withInitial(ThreadContextData::new);

    /** */
    private ThreadContext() {
        // No-op.
    }

    /** */
    public static <T> T get(ThreadContextAttribute<T> attr) {
        return data().get(attr);
    }

    /** */
    public static Scope openNewScope() {
        return DefaultScope.create();
    }

    /** */
    public static <T> Scope withAttribute(ThreadContextAttribute<T> attr, T val) {
        return openNewScope().withAttribute(attr, val);
    }

    /** */
    public static Scope withSnapshot(ThreadContextSnapshot snapshot) {
        return DefaultScope.createWith(snapshot);
    }

    /** */
    public static ThreadContextSnapshot createSnapshot() {
        return data().createSnapshot();
    }

    /** */
    static ThreadContextData data() {
        return THREAD_CONTEXT_DATA_HOLDER.get();
    }
}
