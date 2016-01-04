package org.apache.ignite.internal.processors.hadoop.cls;

import org.apache.hadoop.mapreduce.Job;

/**
 * Class has a direct Hadoop dependency and a circular dependency on another class.
 */
public class CircularDependencyHadoop {
    /** */
    Job[][] jobs = new Job[4][4];

    /** */
    private CircularDependencyNoHadoop y;
}
