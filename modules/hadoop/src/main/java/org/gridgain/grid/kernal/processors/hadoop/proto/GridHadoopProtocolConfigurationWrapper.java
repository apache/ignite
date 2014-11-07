/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.proto;

import org.apache.hadoop.conf.*;

import java.io.*;

/**
 * Configuration wrapper.
 */
public class GridHadoopProtocolConfigurationWrapper implements Externalizable {
    /** Configuration. */
    private Configuration conf;

    /**
     * {@link Externalizable} support.
     */
    public GridHadoopProtocolConfigurationWrapper() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param conf Configuration.
     */
    public GridHadoopProtocolConfigurationWrapper(Configuration conf) {
        this.conf = conf;
    }

    /**
     * @return Underlying configuration.
     */
    public Configuration get() {
        return conf;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        conf.write(out);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        conf = new Configuration();

        conf.readFields(in);
    }
}
