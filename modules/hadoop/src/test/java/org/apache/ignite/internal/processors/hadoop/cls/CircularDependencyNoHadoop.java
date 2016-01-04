package org.apache.ignite.internal.processors.hadoop.cls;

/**
 * Does not have direct Hadoop dependency, but has a circular
 */
public class CircularDependencyNoHadoop {
    /** */
    private CircularDependencyHadoop x;
}
