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

package org.apache.ignite.internal.thread.pool;

import java.util.concurrent.ForkJoinPool;
import org.apache.ignite.internal.thread.context.concurrent.ContextAwareExecutorService;

/** */
public class IgniteForkJoinPool extends ContextAwareExecutorService<ForkJoinPool> {
    /** */
    private static final IgniteForkJoinPool COMMON = new IgniteForkJoinPool(ForkJoinPool.commonPool());

    /** */
    public IgniteForkJoinPool() {
        this(new ForkJoinPool());
    }

    /** */
    public IgniteForkJoinPool(int parallelism) {
        this(new ForkJoinPool(parallelism));
    }

    /** */
    public IgniteForkJoinPool(
        int parallelism,
        ForkJoinPool.ForkJoinWorkerThreadFactory factory,
        Thread.UncaughtExceptionHandler handler,
        boolean asyncMode
    ) {
        this(new ForkJoinPool(parallelism, factory, handler, asyncMode));
    }

    /** */
    private IgniteForkJoinPool(ForkJoinPool delegate) {
        super(delegate);
    }

    /** */
    public static IgniteForkJoinPool commonPool() {
        return COMMON;
    }
}
