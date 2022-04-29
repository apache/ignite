/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.job;

import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Timeout object for delayed {@link GridJobWorker worker} interruption.
 *
 * <p>After calling {@link GridJobWorker#cancel} the worker should try to complete gracefully,
 * if it doesn't then it will {@link Thread#interrupt interrupt} after some time.
 */
public class JobWorkerInterruptionTimeoutObject implements GridTimeoutObject {
    /** Compute job worker. */
    private final GridJobWorker jobWorker;

    /** ID of timeout object. */
    private final IgniteUuid id;

    /** Time when the timeout object should be executed in mills. */
    private final long endTime;

    /**
     * Constructor.
     *
     * @param jobWorker Compute job worker.
     * @param endTime Time when the timeout object should be executed in mills.
     */
    public JobWorkerInterruptionTimeoutObject(
        GridJobWorker jobWorker,
        long endTime
    ) {
        this.jobWorker = jobWorker;
        this.endTime = endTime;

        id = IgniteUuid.randomUuid();
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid timeoutId() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public long endTime() {
        return endTime;
    }

    /** {@inheritDoc} */
    @Override public void onTimeout() {
        assert jobWorker.isCancelled() : jobWorker;

        Thread runner = jobWorker.runner();

        if (runner != null && !jobWorker.isDone())
            runner.interrupt();
    }

    /**
     * @return Compute job worker.
     */
    public GridJobWorker jobWorker() {
        return jobWorker;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JobWorkerInterruptionTimeoutObject.class, this);
    }
}
