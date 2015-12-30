package org.apache.ignite.internal.processors.hadoop.cls;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * Implements a Hadoop interface.
 */
public class HadoopImplements implements Configurable {
    /** {@inheritDoc} */
    @Override public void setConf(Configuration conf) {
        // noop
    }

    /** {@inheritDoc} */
    @Override public Configuration getConf() {
        return null;
    }
}
