/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.shuffle;

import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.*;

import java.util.concurrent.*;

/**
 * TODO write doc
 */
public class GridHadoopShuffle extends GridHadoopComponent {
    /** */
    private ConcurrentMap<?, ?> mapOutputs = new ConcurrentHashMap<>();

    /**
     * @param mapperInfo Mapper info.
     * @return Output for mapper.
     */
    public GridHadoopTaskOutput getMapperOutput(GridHadoopTaskInfo mapperInfo) {
        return null;
    }

    /**
     * @param reducerInfo Reducer info.
     * @return
     */
    public GridHadoopTaskInput getReducerInput(GridHadoopTaskInfo reducerInfo) {
        return null;
    }

    /**
     * @param jobId Job id.
     */
    public void jobFinished(GridHadoopJobId jobId) {

    }
}
