package org.apache.ignite.internal.processors.hadoop.impl;

import org.apache.hadoop.examples.DBCountPageView;
import org.apache.hadoop.util.Tool;

/**
 *
 */
public class HadoopDBCountPageViewExampleTest extends HadoopGenericExampleTest {

    private final GenericHadoopExample ex = new GenericHadoopExample() {
        /** */
        private final DBCountPageView dbCountPageView = new DBCountPageView();

        @Override String[] parameters(FrameworkParameters fp) {
            // No mandatory parameters.
            return new String[0];
        }

        @Override Tool tool() {
            return dbCountPageView;
        }

        @Override void verify(String[] parameters) {
            // Noop, this test is self-verifying, see org.apache.hadoop.examples.DBCountPageView.verify().
        }
    };

    @Override protected GenericHadoopExample example() {
        return ex;
    }
}
