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

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.*;
import org.jetbrains.annotations.*;

/**
 * Hadoop facade implementation.
 */
public class GridHadoopImpl implements GridHadoop {
    /** Hadoop processor. */
    private final GridHadoopProcessor proc;

    /** Busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /**
     * Constructor.
     *
     * @param proc Hadoop processor.
     */
    GridHadoopImpl(GridHadoopProcessor proc) {
        this.proc = proc;
    }

    /** {@inheritDoc} */
    @Override public GridHadoopConfiguration configuration() {
        return proc.config();
    }

    /** {@inheritDoc} */
    @Override public GridHadoopJobId nextJobId() {
        if (busyLock.enterBusy()) {
            try {
                return proc.nextJobId();
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to get next job ID (grid is stopping).");
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> submit(GridHadoopJobId jobId, GridHadoopJobInfo jobInfo) {
        if (busyLock.enterBusy()) {
            try {
                return proc.submit(jobId, jobInfo);
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to submit job (grid is stopping).");
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridHadoopJobStatus status(GridHadoopJobId jobId) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                return proc.status(jobId);
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to get job status (grid is stopping).");
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridHadoopCounters counters(GridHadoopJobId jobId) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                return proc.counters(jobId);
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to get job counters (grid is stopping).");
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteFuture<?> finishFuture(GridHadoopJobId jobId) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                return proc.finishFuture(jobId);
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to get job finish future (grid is stopping).");
    }

    /** {@inheritDoc} */
    @Override public boolean kill(GridHadoopJobId jobId) throws IgniteCheckedException {
        if (busyLock.enterBusy()) {
            try {
                return proc.kill(jobId);
            }
            finally {
                busyLock.leaveBusy();
            }
        }
        else
            throw new IllegalStateException("Failed to kill job (grid is stopping).");
    }
}
