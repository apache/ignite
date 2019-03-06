/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.hadoop;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.HadoopConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopCounters;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.jetbrains.annotations.Nullable;

/**
 * Hadoop facade implementation.
 */
public class HadoopImpl implements Hadoop {
    /** Hadoop processor. */
    private final HadoopProcessor proc;

    /** Busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /**
     * Constructor.
     *
     * @param proc Hadoop processor.
     */
    HadoopImpl(HadoopProcessor proc) {
        this.proc = proc;
    }

    /** {@inheritDoc} */
    @Override public HadoopConfiguration configuration() {
        return proc.config();
    }

    /** {@inheritDoc} */
    @Override public HadoopJobId nextJobId() {
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
    @Override public IgniteInternalFuture<?> submit(HadoopJobId jobId, HadoopJobInfo jobInfo) {
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
    @Nullable @Override public HadoopJobStatus status(HadoopJobId jobId) throws IgniteCheckedException {
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
    @Nullable @Override public HadoopCounters counters(HadoopJobId jobId) throws IgniteCheckedException {
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
    @Nullable @Override public IgniteInternalFuture<?> finishFuture(HadoopJobId jobId) throws IgniteCheckedException {
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
    @Override public boolean kill(HadoopJobId jobId) throws IgniteCheckedException {
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