/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

/**
 * Task context.
 */
public class GridHadoopTaskContext {
    /** */
    private final GridHadoopJob job;

    /** */
    private final GridHadoopTaskInput input;

    /** */
    private final GridHadoopTaskOutput output;

    /**
     * @param job Job.
     * @param input Input.
     * @param output Output.
     */
    public GridHadoopTaskContext(GridHadoopJob job, GridHadoopTaskInput input, GridHadoopTaskOutput output) {
        this.job = job;
        this.input = input;
        this.output = output;
    }

    /**
     * Gets task output.
     *
     * @return Task output.
     */
    public GridHadoopTaskOutput output() {
        return output;
    }

    /**
     * Gets task input.
     *
     * @return Task input.
     */
    public GridHadoopTaskInput input() {
        return input;
    }

    /**
     * @return Job.
     */
    public GridHadoopJob job() {
        return job;
    }
}
