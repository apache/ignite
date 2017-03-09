package org.apache.ignite.internal.processors.hadoop.impl;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.util.Tool;
import org.apache.ignite.internal.processors.hadoop.HadoopJobProperty;
import org.apache.ignite.internal.processors.hadoop.impl.pi2.DistBbp;
import org.apache.ignite.internal.util.typedef.X;

/**
 * Bbp Pi digits example.
 */
public class HadoopBbpExampleTest extends HadoopGenericExampleTest {
    /** */
    private final GenericHadoopExample ex = new GenericHadoopExample() {
        /** */
        private final DistBbp impl = new DistBbp() {
            /** {@inheritDoc} */
            @Override protected void verify(String res) {
                super.verify(res);

                //assertEquals("90FDAA22168C 2 (12 hex digits)", res); // Correct result for b==2.
                assertEquals("110B4611A625 F (12 hex digits)", res); // Correct result for b==25.

                X.println("Verified okay.");
            }
        };

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

            return new String[] { "25", "2", "2", "x", "2",
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
}
