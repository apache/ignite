/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.hadoop.conf.*;
import org.gridgain.grid.hadoop.*;

/**
 * Hadoop job info based on default Hadoop configuration.
 */
public class GridHadoopDefaultJobInfo implements GridHadoopJobInfo {
    /** Configuration. */
    private Configuration cfg;

    /**
     * @param cfg Hadoop configuration.
     */
    public GridHadoopDefaultJobInfo(Configuration cfg) {
        this.cfg = cfg;
    }

    /**
     * @return Hadoop configuration.
     */
    public Configuration configuration() {
        return cfg;
    }
}
