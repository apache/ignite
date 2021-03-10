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

import java.util.concurrent.Executor;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.plugin.extensions.communication.IoPool;
import org.jetbrains.annotations.NotNull;

/** */
public class SecurityAwareIoPool implements IoPool {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final IoPool original;

    /** */
    private final Executor executor;

    /** */
    public SecurityAwareIoPool(GridKernalContext ctx, IoPool original) {
        this.ctx = ctx;
        this.original = original;

        executor = new Executor() {
            @Override public void execute(@NotNull Runnable cmd) {
                Runnable r = cmd;

                if (SecurityUtils.isAuthentificated(SecurityAwareIoPool.this.ctx))
                    r = new SecurityAwareRunnable(SecurityAwareIoPool.this.ctx.security(), cmd);

                SecurityAwareIoPool.this.original.executor().execute(r);
            }
        };
    }

    /** {@inheritDoc} */
    @Override public byte id() {
        return original.id();
    }

    /** {@inheritDoc} */
    @Override public Executor executor() {
        return executor;
    }
}
