package org.apache.ignite.internal.processors.hadoop.cls;

import org.apache.hadoop.fs.FileSystem;

/**
 * Method contains a Hadoop type method invocation.
 */
public class HadoopMethodInvocation {
    /** */
    void foo(FileSystem fs) {
        fs.getChildFileSystems();
    }
}
