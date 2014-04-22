/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import org.gridgain.grid.*;

import java.io.*;

/**
 * TODO write doc
 */
public interface GridHadoopTask extends Serializable {
    /**
     * @return task info.
     */
    public GridHadoopTaskInfo info();

    /**
     *
     *
     * @param ctx Context.
     */
    public void run(GridHadoopTaskContext ctx) throws GridInterruptedException, GridException;
}
