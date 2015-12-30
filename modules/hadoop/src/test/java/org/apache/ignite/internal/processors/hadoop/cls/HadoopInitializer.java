package org.apache.ignite.internal.processors.hadoop.cls;

/**
 * Has a field initialized with an expression invoking Hadoop method.
 */
public class HadoopInitializer {
    /** */
    private final Object x = org.apache.hadoop.fs.FileSystem.getDefaultUri(null);

    /** */
    HadoopInitializer() throws Exception {
        // noop
    }
}
