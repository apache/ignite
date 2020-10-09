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

package org.apache.ignite.failure;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.worker.GridWorker;

/**
 * Thrown if GridWorker is blocked. Contains stacktrace of blocked worker.
 */
public class BlockedThreadException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final GridWorker worker;

    /**
     * @param blockedWorker Blocked system worker.
     */
    public BlockedThreadException(GridWorker blockedWorker) {
        super(S.toString(GridWorker.class, blockedWorker));

        worker = blockedWorker;

        ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();

        Thread runner = worker.runner();

        if (runner != null) {
            ThreadInfo info = mxBean.getThreadInfo(runner.getId(), Integer.MAX_VALUE);

            if (info != null)
                setStackTrace(info.getStackTrace());
        }
    }

    /**
     * @return Blocked worker that causes this exception.
     */
    public GridWorker worker() {
        return worker;
    }
}
