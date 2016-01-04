package org.apache.ignite.internal.processors.hadoop.cls;

import org.apache.hadoop.conf.Configuration;

/**
 * Has a local variable of Hadoop type.
 */
public class HadoopLocalVariableType {
    /** */
    void foo() {
        Configuration c = null;

        moo(c);
    }

    /** */
    void moo(Object x) {

    }
}
