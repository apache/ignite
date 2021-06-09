/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util.concurrent;

import java.util.concurrent.TimeUnit;

/**
 * Striped executor.
 */
public interface FixedThreadsExecutorGroup extends Iterable<SingleThreadExecutor> {

    /**
     * Returns one of the {@link SingleThreadExecutor}s managed by this {@link FixedThreadsExecutorGroup}.
     */
    SingleThreadExecutor next();

    /**
     * Executes the given task at some time in the future.  The task execute by a specified thread, which is selected by
     * index.
     *
     * @param index index for thread chooser
     * @param task the runnable task
     */
    void execute(final int index, final Runnable task);

    /**
     * Selects an executor by index.
     *
     * @param index The index.
     * @return The executor.
     */
    SingleThreadExecutor select(final int index);

    /**
     * Shortcut method for {@link #shutdownGracefully(long, TimeUnit)} with sensible default values.
     *
     * @return true if success to shutdown
     */
    boolean shutdownGracefully();

    /**
     * Signals all executors that the caller wants them to be shutdown.
     *
     * @param timeout the maximum amount of time to wait until the executor is shutdown
     * @param unit the unit of {@code timeout}
     * @return true if success to shutdown
     */
    boolean shutdownGracefully(final long timeout, final TimeUnit unit);
}
