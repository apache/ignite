/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.jobtracker;

import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.*;

import java.io.*;
import java.util.*;

/**
 * TODO write doc
 */
public class GridHadoopJobContext {

    public GridHadoopJob job() {
        return null;
    }

    public GridHadoopJobId jobId() {
        return null;
    }

    public List<GridHadoopTaskInfo> mappers() {
        return null;
    }

    public List<Serializable> mapsHandler() {
        return null;
    }
}
