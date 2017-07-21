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

package org.apache.ignite.internal.util.lang;

import org.apache.ignite.lang.IgniteRunnable;

/**
 * Defines a convenient absolute, i.e. {@code no-arg} and {@code no return value} closure. This closure
 * that has {@code void} return type and no arguments (free variables).
 * <h2 class="header">Thread Safety</h2>
 * Note that this interface does not impose or assume any specific thread-safety by its
 * implementations. Each implementation can elect what type of thread-safety it provides,
 * if any.
 * <p>
 * Note that this class implements {@link org.apache.ignite.compute.ComputeJob} interface for convenience and can be
 * used in {@link org.apache.ignite.compute.ComputeTask} implementations directly, if needed, as an alternative to
 * {@link org.apache.ignite.compute.ComputeJobAdapter}.
 * @see GridFunc
 */
public abstract class GridAbsClosure implements IgniteRunnable {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Absolute closure body.
     */
    public abstract void apply();

    /**
     * Delegates to {@link #apply()} method.
     * <p>
     * {@inheritDoc}
     */
    @Override public final void run() {
        apply();
    }
}