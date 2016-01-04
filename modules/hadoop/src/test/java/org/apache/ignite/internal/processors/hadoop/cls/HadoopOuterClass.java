package org.apache.ignite.internal.processors.hadoop.cls;

import org.apache.hadoop.conf.Configuration;

/**
 * Outer class depends on Hadoop, but Inner *static* one does not.
 */
public class HadoopOuterClass {
    /** */
    Configuration c;

    /** */
    public static class InnerNoHadoop {
        /** */
        int x;

        /** */
        void foo() {}
    }
}
