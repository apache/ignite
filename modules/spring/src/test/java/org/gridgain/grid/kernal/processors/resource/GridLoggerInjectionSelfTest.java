/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.resource;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;

/**
 * Test for injected logger category.
 */
public class GridLoggerInjectionSelfTest extends GridCommonAbstractTest implements Externalizable {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * Test that closure gets right log category injected on all nodes using field injection.
     *
     * @throws Exception If failed.
     */
    public void testClosureField() throws Exception {
        Ignite ignite = grid(0);

        ignite.compute().call(new IgniteCallable<Object>() {
            @IgniteLoggerResource(categoryClass = GridLoggerInjectionSelfTest.class)
            private IgniteLogger log;

            @Override public Object call() throws Exception {
                if (log instanceof GridLoggerProxy) {
                    Object category = U.field(log,  "ctgr");

                    assertTrue("Logger created for the wrong category.",
                        category.toString().contains(GridLoggerInjectionSelfTest.class.getName()));
                }
                else
                    fail("This test should be run with proxy logger.");

                return null;
            }
        });
    }

    /**
     * Test that closure gets right log category injected on all nodes using method injection.
     *
     * @throws Exception If failed.
     */
    public void testClosureMethod() throws Exception {
        Ignite ignite = grid(0);

        ignite.compute().call(new IgniteCallable<Object>() {
            @IgniteLoggerResource(categoryClass = GridLoggerInjectionSelfTest.class)
            private void log(IgniteLogger log) {
                if (log instanceof GridLoggerProxy) {
                    Object category = U.field(log,  "ctgr");

                    assertTrue("Logger created for the wrong category.",
                        category.toString().contains(GridLoggerInjectionSelfTest.class.getName()));
                }
                else
                    fail("This test should be run with proxy logger.");
            }

            @Override public Object call() throws Exception {
                return null;
            }
        });
    }

    /**
     * Test that closure gets right log category injected through {@link org.apache.ignite.resources.IgniteLoggerResource#categoryName()}.
     *
     * @throws Exception If failed.
     */
    public void testStringCategory() throws Exception {
        Ignite ignite = grid(0);

        ignite.compute().call(new IgniteCallable<Object>() {
            @IgniteLoggerResource(categoryName = "GridLoggerInjectionSelfTest")
            private void log(IgniteLogger log) {
                if (log instanceof GridLoggerProxy) {
                    Object category = U.field(log,  "ctgr");

                    assertTrue("Logger created for the wrong category.",
                        "GridLoggerInjectionSelfTest".equals(category.toString()));
                }
                else
                    fail("This test should be run with proxy logger.");
            }

            @Override public Object call() throws Exception {
                return null;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // No-op.
    }
}
