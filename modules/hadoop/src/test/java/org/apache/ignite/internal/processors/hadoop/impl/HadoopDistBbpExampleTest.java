package org.apache.ignite.internal.processors.hadoop.impl;

import org.apache.hadoop.examples.BaileyBorweinPlouffe;
import org.apache.hadoop.util.Tool;

/**
 * Distributed Bbp Pi digits example.
 */
public class HadoopDistBbpExampleTest extends HadoopGenericExampleTest {
    /** */
    private final GenericHadoopExample ex = new GenericHadoopExample() {
        private final BaileyBorweinPlouffe impl = new BaileyBorweinPlouffe();

        @Override String name() {
            return "bbp";
        }

        @Override String[] parameters(FrameworkParameters fp) {
            // <startDigit> <nDigits> <nMaps> <workingDir> :
            return new String[] { "1", "48", String.valueOf(fp.numMaps()), fp.getWorkDir(name()) };
        }

        @Override Tool tool() {
            return impl;
        }

        @Override void verify(String[] parameters) {
            // TODO: implement
        }
    };

    /** {@inheritDoc} */
    @Override protected GenericHadoopExample example() {
        return ex;
    }
}
