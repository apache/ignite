package org.apache.ignite.internal.processors.hadoop.impl;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.examples.dancing.DistributedPentomino;
//import org.apache.hadoop.examples.dancing.OneSidedPentomino;
//import org.apache.hadoop.examples.dancing.Pentomino;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Pertomino example in form of test.
 */
public class HadoopDistributedPentominoExampleTest extends HadoopGenericExampleTest {

    @Override protected long getTestTimeout() {
        return super.getTestTimeout() * 10;
    }

    /** */
    private final GenericHadoopExample ex = new GenericHadoopExample() {
        private final DistributedPentomino2 impl = new DistributedPentomino2();

        @Override String[] parameters(FrameworkParameters fp) {
            // pentomino <output> [-depth #] [-height #] [-width #]
            return new String[] {
                outDir(fp),

//                "-depth", "2", hangs.
//                "-width", "8",
//                "-height", "9",

                "-depth", "2",
                "-width", "8",
                "-height", "8",

//                "-depth", "5",
//                "-width", "9",
//                "-height", "10",
            };
        }

        @Override Tool tool() {
            return impl;
        }

        @Override void verify(String[] parameters) {
            // TODO
        }
    };

    /** {@inheritDoc} */
    @Override protected void prepareConf(Configuration conf) {
        super.prepareConf(conf);

        conf.set(Pentomino2.CLASS, OneSidedPentomino2.class.getName());
        conf.set(MRJobConfig.NUM_MAPS, "200");
    }

    /** {@inheritDoc} */
    @Override protected GenericHadoopExample example() {
        return ex;
    }
}
