/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.startup.cmdline;

import org.apache.ignite.startup.cmdline.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.*;

/**
 * GridCommandLineTransformer test.
 */
public class GridCommandLineTransformerSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testTransformIfNoArguments() throws Exception {
        assertEquals(
            "\"INTERACTIVE=0\" \"QUIET=-DGRIDGAIN_QUIET=true\" \"NO_PAUSE=0\" " +
            "\"JVM_XOPTS=\" \"CONFIG=\"",
            GridCommandLineTransformer.transform());
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformIfArgumentIsnull() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @SuppressWarnings("NullArgumentToVariableArgMethod")
            @Override public Object call() throws Exception {
                return GridCommandLineTransformer.transform(null);
            }
        }, AssertionError.class, null);
    }

    /**
     * Checks that first unrecognized option is treated without error (we assume it's a path to a config file) but the
     * next one leads to error.
     *
     * @throws Exception If failed.
     */
    public void testTransformIfUnsupportedOptions() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return GridCommandLineTransformer.transform("-z", "qwerty", "asd");
            }
        }, RuntimeException.class, "Unrecognised parameter has been found: qwerty");
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformIfUnsupportedJvmOptions() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return GridCommandLineTransformer.transform("-J-Xmx1g", "-J-XX:OnError=\"dir c:\\\"");
            }
        }, RuntimeException.class, GridCommandLineTransformer.JVM_OPTION_PREFIX +
            " JVM parameters for GridGain batch scripts " +
            "with double quotes are not supported. " +
            "Use JVM_OPTS environment variable to pass any custom JVM option.");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return GridCommandLineTransformer.transform("-J-Xmx1g", "-J-XX:OnOutOfMemoryError=\"dir c:\\\"");
            }
        }, RuntimeException.class, GridCommandLineTransformer.JVM_OPTION_PREFIX +
            " JVM parameters for GridGain batch scripts " +
            "with double quotes are not supported. " +
            "Use JVM_OPTS environment variable to pass any custom JVM option.");
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformIfSeveralArgumentsWithoutDashPrefix() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return GridCommandLineTransformer.transform("c:\\qw.xml", "abc", "d");
            }
        }, RuntimeException.class, "Unrecognised parameter has been found: abc");
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformIfOnlyPathToConfigSpecified() throws Exception {
        assertEquals(
            "\"INTERACTIVE=0\" \"QUIET=-DGRIDGAIN_QUIET=true\" \"NO_PAUSE=0\" " +
            "\"JVM_XOPTS=\" \"CONFIG=c:\\qw.xml\"",
            GridCommandLineTransformer.transform("c:\\qw.xml"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformIfAllSupportedArguments() throws Exception {
        assertEquals(
            "\"INTERACTIVE=1\" \"QUIET=-DGRIDGAIN_QUIET=false\" \"NO_PAUSE=1\" " +
            "\"JVM_XOPTS=-Xmx1g -Xms1m\" " +
            "\"CONFIG=\"c:\\path to\\русский каталог\"\"",
            GridCommandLineTransformer.transform("-i", "-np", "-v", "-J-Xmx1g", "-J-Xms1m",
            "\"c:\\path to\\русский каталог\""));
    }
}
