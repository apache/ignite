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

package org.apache.ignite.internal.benchmarks.jmh.profile;

import java.util.LinkedHashMap;
import java.util.Map;

/** Profiler actions run before the nodes stop: JMH runs the teardown before the last {@code afterIteration}. */
public final class JmhNodeStopHook {
    /** Error of a profiler whose benchmark stopped the nodes without the hook. */
    static final String NOT_CALLED = "The benchmark stopped its Ignite nodes without calling JmhNodeStopHook.run() " +
        "first, so the profiler can't count the last iteration.";

    /** Action per profiler class, in JMH's {@code -prof} order; a new instance replaces its class's action in place. */
    private static final Map<Class<?>, Runnable> ACTIONS = new LinkedHashMap<>();

    /** */
    private JmhNodeStopHook() {
        // No-op.
    }

    /**
     * @param owner Profiler class.
     * @param action Action to run before the nodes stop.
     */
    public static void set(Class<?> owner, Runnable action) {
        ACTIONS.put(owner, action);
    }

    /** Runs the actions; benchmarks call it first thing in their teardown. */
    public static void run() {
        ACTIONS.values().forEach(Runnable::run);
    }
}
