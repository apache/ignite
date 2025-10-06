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

package org.apache.ignite.internal.thread.context.pool;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.worker.GridWorkerListener;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.thread.context.function.ThreadContextAwareRunnable;
import org.jetbrains.annotations.NotNull;

/** */
public class ThreadContextAwareStripedExecutor extends StripedExecutor {
    /** */
    public ThreadContextAwareStripedExecutor(
        int cnt,
        String igniteInstanceName,
        String poolName,
        IgniteLogger log,
        IgniteInClosure<Throwable> errHnd,
        GridWorkerListener gridWorkerLsnr,
        long failureDetectionTimeout
    ) {
        super(cnt, igniteInstanceName, poolName, log, errHnd, gridWorkerLsnr, failureDetectionTimeout);
    }

    /** */
    public ThreadContextAwareStripedExecutor(
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
    }

    /** {@inheritDoc} */
    @Override public void execute(@NotNull Runnable cmd) {
        super.execute(ThreadContextAwareRunnable.wrap(cmd));
    }

    /** {@inheritDoc} */
    @Override public void execute(int idx, Runnable cmd) {
        super.execute(idx, ThreadContextAwareRunnable.wrap(cmd));
    }
}
