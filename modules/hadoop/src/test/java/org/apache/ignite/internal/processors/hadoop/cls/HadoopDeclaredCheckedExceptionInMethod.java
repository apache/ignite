package org.apache.ignite.internal.processors.hadoop.cls;

import org.apache.hadoop.fs.ChecksumException;

/**
 * Method declares a checked Hadoop Exception.
 */
public class HadoopDeclaredCheckedExceptionInMethod {
    /** */
    void foo() throws ChecksumException {
        // noop
    }
}
