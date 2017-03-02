package org.apache.ignite.internal.processors.hadoop.impl;

import org.apache.hadoop.examples.DBCountPageView;
import org.apache.hadoop.util.Tool;

/**
 *
 */
public class HadoopDBCountPageViewExampleTest extends HadoopGenericExampleTest {
    /** */
    private final GenericHadoopExample ex = new GenericHadoopExample() {
        /** */
        private final DBCountPageView dbCntPageView = new DBCountPageView();

        /** {@inheritDoc} */
        @Override String[] parameters(FrameworkParameters fp) {
            // No mandatory parameters.
            return new String[0];
        }

        /** {@inheritDoc} */
        @Override Tool tool() {
            return dbCntPageView;
        }

        /** {@inheritDoc} */
        @Override void verify(String[] parameters) {
            // Noop, this test is self-verifying, see org.apache.hadoop.examples.DBCountPageView.verify().
        }
    };

    /** {@inheritDoc} */
    @Override protected GenericHadoopExample example() {
        return ex;
    }
}
