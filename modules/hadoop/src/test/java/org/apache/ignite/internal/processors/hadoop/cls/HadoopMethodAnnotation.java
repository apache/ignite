package org.apache.ignite.internal.processors.hadoop.cls;

/**
 * Method has a Hadoop annotation.
 */
public class HadoopMethodAnnotation {
    /** */
    @org.apache.hadoop.classification.InterfaceStability.Unstable
    void foo() {
        // noop
    }
}
