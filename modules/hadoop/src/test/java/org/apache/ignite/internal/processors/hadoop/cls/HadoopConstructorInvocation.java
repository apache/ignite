package org.apache.ignite.internal.processors.hadoop.cls;

import org.apache.hadoop.conf.Configuration;

/**
 * Invokes a Hadoop type constructor.
 */
public class HadoopConstructorInvocation {
    /** */
    private void foo() {
        Object x = new Configuration();
    }
}
