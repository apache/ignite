package org.apache.ignite.internal.processors.hadoop.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.pi.DistBbp;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.util.Tool;
import org.apache.ignite.internal.processors.hadoop.HadoopJobProperty;

/**
 * Bbp Pi digits example.
 */
public class HadoopBbpExampleTest extends HadoopGenericExampleTest {
    /** */
    private final GenericHadoopExample ex = new GenericHadoopExample() {
        /** */
        private final Tool impl = new DistBbp();

        /** {@inheritDoc} */
        @Override String[] parameters(FrameworkParameters fp) {
//      Usage: java org.apache.hadoop.examples.pi.DistBbp <b> <nThreads> <nJobs> <type> <nPart> <remoteDir> <localDir>
//            <b> The number of bits to skip, i.e. compute the (b+1)th position.
//            <nThreads> The number of working threads.
//            <nJobs> The number of jobs per sum.
//            <type> 'm' for map side job, 'r' for reduce side job, 'x' for mix type.
//            <nPart> The number of parts per job.
//            <remoteDir> Remote directory for submitting jobs.
//            <localDir> Local directory for storing output files.

            return new String[] { "2", "2", "2", "x", "2",
                fp.getWorkDir(name()) + "/remote" ,
                fp.getWorkDir(name()) + "/local" };
        }

        /** {@inheritDoc} */
        @Override Tool tool() {
            return impl;
        }

        /** {@inheritDoc} */
        @Override void verify(String[] parameters) {
            // The example writes actual result to Util.out stream, so it's difficult to
            // catch it to check.
        }
    };

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 3600 * 1000;
    }

    /** {@inheritDoc} */
    @Override protected void prepareConf(Configuration conf) {
        super.prepareConf(conf);

        // See org.apache.hadoop.examples.pi.Parser.VERBOSE_PROPERTY
        conf.set("pi.parser.verbose", "true");

        conf.set("pi.job.separation.seconds", "0");

        conf.set(MRConfig.FRAMEWORK_NAME, "ignite");
        conf.set(MRConfig.MASTER_ADDRESS, "localhost:11211");

        // See https://issues.apache.org/jira/browse/IGNITE-4720:
        conf.set(HadoopJobProperty.JOB_SHARED_CLASSLOADER.propertyName(), "false");
    }

    /** {@inheritDoc} */
    @Override protected GenericHadoopExample example() {
        return ex;
    }

    @Override protected boolean isOneJvm() {
        return true; // TODO
    }

    @Override protected int gridCount() {
        return 1;
    }
}
