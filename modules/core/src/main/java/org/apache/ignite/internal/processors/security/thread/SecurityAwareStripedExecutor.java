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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.worker.GridWorkerListener;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.NotNull;

/**
 * Extends {@link StripedExecutor} with the ability to execute tasks in security context that was actual when task was
 * added to executor's queue.
 */
public class SecurityAwareStripedExecutor extends StripedExecutor {
    /** */
    private final IgniteSecurity security;

    /** */
    public SecurityAwareStripedExecutor(
        IgniteSecurity security,
        int cnt,
        String igniteInstanceName,
        String poolName,
        IgniteLogger log,
        IgniteInClosure<Throwable> errHnd,
        GridWorkerListener gridWorkerLsnr,
        long failureDetectionTimeout
    ) {
        super(cnt, igniteInstanceName, poolName, log, errHnd, gridWorkerLsnr, failureDetectionTimeout);

        this.security = security;
    }

    /** */
    public SecurityAwareStripedExecutor(
        IgniteSecurity security,
        int cnt,
        String igniteInstanceName,
        String poolName,
        IgniteLogger log,
        IgniteInClosure<Throwable> errHnd,
        boolean stealTasks,
        GridWorkerListener gridWorkerLsnr,
        long failureDetectionTimeout
    ) {
        super(cnt, igniteInstanceName, poolName, log, errHnd, stealTasks, gridWorkerLsnr, failureDetectionTimeout);

        this.security = security;
    }

    /** {@inheritDoc} */
    @Override public void execute(int idx, Runnable cmd) {
        super.execute(idx, SecurityAwareRunnable.of(security, cmd));
    }

    /** {@inheritDoc} */
    @Override public void execute(@NotNull Runnable cmd) {
        super.execute(SecurityAwareRunnable.of(security, cmd));
    }
}
