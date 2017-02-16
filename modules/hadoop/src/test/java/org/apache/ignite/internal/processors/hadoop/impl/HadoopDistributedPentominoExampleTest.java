package org.apache.ignite.internal.processors.hadoop.impl;

import org.apache.hadoop.examples.dancing.DistributedPentomino;
import org.apache.hadoop.util.Tool;

/**
 * Pertomino example in form of test.
 */
public class HadoopDistributedPentominoExampleTest extends HadoopGenericExampleTest {
    /** */
    private final GenericHadoopExample ex = new GenericHadoopExample() {
        private final DistributedPentomino impl = new DistributedPentomino();

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
    @Override protected GenericHadoopExample example() {
        return ex;
    }
}
