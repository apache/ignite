package org.apache.ignite.internal.processors.hadoop.impl;

import org.apache.hadoop.examples.dancing.DistributedPentomino;
import org.apache.hadoop.util.Tool;

/**
 *
 */
public class HadoopDistributedPentominoTest extends HadoopGenericTest {

    private final GenericHadoopExample ex = new GenericHadoopExample() {
        private final DistributedPentomino impl = new DistributedPentomino();

        @Override String name() {
            return "DistributedPentomino";
        }

        @Override String[] parameters(FrameworkParameters fp) {
            // pentomino <output> [-depth #] [-height #] [-width #]
            return new String[] { fp.getWorkDir(name()) + "/out", "-depth", "3", "-height", "4", "-width", "5" };
        }

        @Override Tool tool() {
            return impl;
        }

        @Override void verify(String[] parameters) {
            // TODO
        }
    };

    @Override protected GenericHadoopExample example() {
        return ex;
    }
}
