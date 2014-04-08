/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor;

import org.gridgain.grid.kernal.processors.hadoop.*;

import java.util.*;

/**
 * TODO write doc
 */
public class GridHadoopTaskInfo {

    UUID nodeId() {
        return null;
    }

    GridHadoopTask.Type type() {
        return null;
    }

    GridHadoopJobId jobId() {
        return null;
    }

    GridHadoopJob job() {
        return null;
    }

    int taskNumber() {
        return 0;
    }

    int attempt() {
        return 0;
    }
}
