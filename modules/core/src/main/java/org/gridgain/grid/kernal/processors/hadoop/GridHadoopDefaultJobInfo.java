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

import java.io.*;

/**
 * Hadoop job info based on default Hadoop configuration.
 */
public class GridHadoopDefaultJobInfo implements GridHadoopJobInfo, Externalizable {
    /** Configuration. */
    private Configuration cfg;

    /**
     * Default constructor required by {@link Externalizable}.
     */
    public GridHadoopDefaultJobInfo() {
        // No-op.
    }

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

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        cfg.write(out);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        cfg = new Configuration();

        cfg.readFields(in);
    }
}
