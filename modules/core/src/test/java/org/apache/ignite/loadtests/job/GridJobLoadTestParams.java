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