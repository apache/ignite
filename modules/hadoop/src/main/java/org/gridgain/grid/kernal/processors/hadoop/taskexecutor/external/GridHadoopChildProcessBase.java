/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external;

/**
 * Hadoop process base.
 */
public abstract class GridHadoopChildProcessBase {
    /** Command line args. */
    private String[] args;

    /**
     * @param args Arguments.
     */
    protected GridHadoopChildProcessBase(String[] args) {
        this.args = args;
    }

    /**
     * Starts
     */
    public void start() {

    }
}
