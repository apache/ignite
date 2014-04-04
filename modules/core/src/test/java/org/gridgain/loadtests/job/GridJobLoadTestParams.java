/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.job;

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
