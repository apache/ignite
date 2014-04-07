/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.*;

/**
 * TODO write doc
 */
public class GridHadoopProcessor extends GridProcessorAdapter {
    /**
     * @param ctx Kernal context.
     */
    protected GridHadoopProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    public GridFuture<?> submit(JobConf jobCfg) {

    }

    public GridFuture<?> submit(Job job) {

    }
}
