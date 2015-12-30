package org.apache.ignite.internal.processors.hadoop.cls;

/**
 * Has a paramater annotated with a Hadoop annotation.
 */
public class HadoopParameterAnnotation {
    /** */
    void foo(@org.apache.hadoop.classification.InterfaceStability.Stable Object annotatedParam) {
        // noop
    }
}
