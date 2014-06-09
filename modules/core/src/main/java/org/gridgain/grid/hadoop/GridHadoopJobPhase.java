/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

/**
 * Job run phase.
 */
public enum GridHadoopJobPhase {
    /** Job is running setup task. */
    PHASE_SETUP,

    /** Job is running map and combine tasks. */
    PHASE_MAP,

    /** Job has finished all map tasks and running reduce tasks. */
    PHASE_REDUCE,

    /** Job is stopping due to exception during any of the phases. */
    PHASE_CANCELLING,

    /** Job has finished execution. */
    PHASE_COMPLETE
}
