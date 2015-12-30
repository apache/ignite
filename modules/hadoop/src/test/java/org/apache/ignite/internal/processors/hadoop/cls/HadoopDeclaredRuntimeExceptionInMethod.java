package org.apache.ignite.internal.processors.hadoop.cls;

import org.apache.hadoop.HadoopIllegalArgumentException;

/**
 * Method declares a runtime Hadoop Exception.
 */
public class HadoopDeclaredRuntimeExceptionInMethod {
    /** */
    void foo() throws HadoopIllegalArgumentException {
        // noop
    }
}
