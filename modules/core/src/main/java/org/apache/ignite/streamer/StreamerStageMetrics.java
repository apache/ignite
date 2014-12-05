/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.streamer;

/**
 * Streamer stage metrics.
 */
public interface StreamerStageMetrics {
    /**
     * Gets stage name.
     *
     * @return Stage name.
     */
    public String name();

    /**
     * Gets stage minimum execution time.
     *
     * @return Stage minimum execution time.
     */
    public long minimumExecutionTime();

    /**
     * Gets stage maximum execution time.
     *
     * @return Stage maximum execution time.
     */
    public long maximumExecutionTime();

    /**
     * Gets stage average execution time.
     *
     * @return Stage average execution time.
     */
    public long averageExecutionTime();

    /**
     * Gets stage minimum waiting time.
     *
     * @return Stage minimum waiting time.
     */
    public long minimumWaitingTime();

    /**
     * Gets stage maximum waiting time.
     *
     * @return Stage maximum waiting time.
     */
    public long maximumWaitingTime();

    /**
     * Stage average waiting time.
     *
     * @return Stage average waiting time.
     */
    public long averageWaitingTime();

    /**
     * Gets total stage execution count since last reset.
     *
     * @return Number of times this stage was executed.
     */
    public long totalExecutionCount();

    /**
     * Gets stage failure count.
     *
     * @return Stage failure count.
     */
    public int failuresCount();

    /**
     * Gets flag indicating if stage is being currently executed by at least one thread on current node.
     *
     * @return {@code True} if stage is executing now.
     */
    public boolean executing();
}
