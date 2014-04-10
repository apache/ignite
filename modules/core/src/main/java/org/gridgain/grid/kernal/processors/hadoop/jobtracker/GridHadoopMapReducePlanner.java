/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.jobtracker;

import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * TODO write doc
 */
public interface GridHadoopMapReducePlanner {
    GridHadoopMapReducePlan preparePlan(Collection<GridGgfsBlockLocation> blocks, Collection<GridNode> top,
        GridHadoopJobInfo jobInfo, @Nullable GridHadoopMapReducePlan oldPlan);
}
