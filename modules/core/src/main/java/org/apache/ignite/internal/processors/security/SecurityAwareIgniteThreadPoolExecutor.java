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

package org.apache.ignite.internal.processors.security;

import java.util.concurrent.BlockingQueue;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.thread.IgniteThreadPoolExecutorService;

/**
 *
 */
public class SecurityAwareIgniteThreadPoolExecutor extends SecurityAwareExecutorService
    implements IgniteThreadPoolExecutorService {
    /** */
    public static SecurityAwareHolder<IgniteThreadPoolExecutorService> holder(GridKernalContext ctx,
        IgniteThreadPoolExecutorService original) {
        return new SecurityAwareHolder<IgniteThreadPoolExecutorService>(ctx, original) {
            @Override protected IgniteThreadPoolExecutorService createSecurityAwareInstance() {
                return new SecurityAwareIgniteThreadPoolExecutor(ctx, original);
            }
        };
    }

    /** */
    private final IgniteThreadPoolExecutorService original;

    /** */
    public SecurityAwareIgniteThreadPoolExecutor(GridKernalContext ctx, IgniteThreadPoolExecutorService original) {
        super(ctx, original);

        this.original = original;
    }

    /** {@inheritDoc} */
    @Override public BlockingQueue<Runnable> getQueue() {
        return original.getQueue();
    }
}
