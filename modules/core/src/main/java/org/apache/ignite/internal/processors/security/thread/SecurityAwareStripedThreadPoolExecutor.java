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

package org.apache.ignite.internal.processors.security.thread;

import org.apache.ignite.internal.cache.context.SessionContextProcessor;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.thread.IgniteStripedThreadPoolExecutor;

/**
 * Extends {@link IgniteStripedThreadPoolExecutor} with the ability to execute tasks in security context that was actual
 * when task was added to executor's queue.
 */
public class SecurityAwareStripedThreadPoolExecutor extends IgniteStripedThreadPoolExecutor {
    /** */
    private final IgniteSecurity security;

    /** */
    private final SessionContextProcessor sesCtx;

    /** */
    public SecurityAwareStripedThreadPoolExecutor(
        IgniteSecurity security,
        SessionContextProcessor sesCtx,
        int concurrentLvl, 
        String igniteInstanceName, 
        String threadNamePrefix,
        Thread.UncaughtExceptionHandler eHnd, 
        boolean allowCoreThreadTimeOut, 
        long keepAliveTime
    ) {
        super(concurrentLvl, igniteInstanceName, threadNamePrefix, eHnd, allowCoreThreadTimeOut, keepAliveTime);
        this.sesCtx = sesCtx;
        this.security = security;
    }

    /** {@inheritDoc} */
    @Override public void execute(Runnable task, int idx) {
        super.execute(SecurityAwareRunnable.of(security, sesCtx, task), idx);
    }
}
