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

package org.apache.ignite.loadtests.job;

/**
 * Test task parameters.
 */
public class GridJobLoadTestParams {
    /** Number of jobs to be spawned. */
    private final int jobsCnt;

    /** Duration between job start and failure check. */
    private final long executionDuration;

    /** Duration between failure check and job completion. */
    private final int completionDelay;

    /** Probability of simulated job failure. */
    private final double jobFailureProbability;

    /**
     * @param jobsCnt Number of jobs to be spawned.
     * @param executionDuration Duration between job start and failure check.
     * @param completionDelay Duration between failure check and job completion.
     * @param jobFailureProbability Probability of simulated job failure.
     */
    public GridJobLoadTestParams(int jobsCnt, long executionDuration, int completionDelay, double jobFailureProbability) {
        this.jobsCnt = jobsCnt;
        this.executionDuration = executionDuration;
        this.completionDelay = completionDelay;
        this.jobFailureProbability = jobFailureProbability;
    }

    /**
     * Returns number of jobs to be spawned.
     *
     * @return Number of jobs to be spawned.
     */
    public int getJobsCount() {
        return jobsCnt;
    }

    /**
     * Returns duration between job start and failure check.
     *
     * @return Duration between job start and failure check.
     */
    public long getExecutionDuration() {
        return executionDuration;
    }

    /**
     * Returns duration between failure check and job completion.
     *
     * @return Duration between failure check and job completion.
     */
    public int getCompletionDelay() {
        return completionDelay;
    }

    /**
     * Returns probability of simulated job failure.
     *
     * @return Probability of simulated job failure.
     */
    public double getJobFailureProbability() {
        return jobFailureProbability;
    }
}