package org.apache.ignite.internal.processors.hadoop.impl;

import org.apache.hadoop.examples.pi.DistBbp;
import org.apache.hadoop.util.Tool;

/**
 *
 */
public class HadoopBbpTest extends HadoopGenericTest {

    private final GenericHadoopExample ex = new GenericHadoopExample() {
        private final Tool impl = new DistBbp();

        @Override String name() {
            return "bbp";
        }

        @Override String[] parameters(FrameworkParameters fp) {
//            Usage: java org.apache.hadoop.examples.pi.DistBbp <b> <nThreads> <nJobs> <type> <nPart> <remoteDir> <localDir>
//            <b> The number of bits to skip, i.e. compute the (b+1)th position.
//            <nThreads> The number of working threads.
//            <nJobs> The number of jobs per sum.
//                <type> 'm' for map side job, 'r' for reduce side job, 'x' for mix type.
//            <nPart> The number of parts per job.
//                <remoteDir> Remote directory for submitting jobs.
//            <localDir> Local directory for storing output files.

            return new String[] { "0",  "2", "2", "x", "2",
                fp.getWorkDir(name()) + "/remote" ,
                fp.getWorkDir(name()) + "/local" };
        }

        @Override Tool tool() {
            return impl;
        }

        @Override void verify(String[] parameters) {
            // TODO: implement
        }
    };

    @Override protected GenericHadoopExample example() {
        return ex;
    }
}
