/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.startup.cmdline;

import java.util.concurrent.Callable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * GridCommandLineTransformer test.
 */
public class GridCommandLineTransformerSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testTransformIfNoArguments() throws Exception {
        assertEquals(
            "\"INTERACTIVE=0\" \"QUIET=-DIGNITE_QUIET=true\" \"NO_PAUSE=0\" " +
                "\"NO_JMX=0\" \"JVM_XOPTS=\" \"CONFIG=\"",
            CommandLineTransformer.transform());
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformIfArgumentIsnull() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @SuppressWarnings("NullArgumentToVariableArgMethod")
            @Override public Object call() throws Exception {
                return CommandLineTransformer.transform(null);
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
                return CommandLineTransformer.transform("-z", "qwerty", "asd");
            }
        }, RuntimeException.class, "Unrecognised parameter has been found: qwerty");
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformIfUnsupportedJvmOptions() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return CommandLineTransformer.transform("-J-Xmx1g", "-J-XX:OnError=\"dir c:\\\"");
            }
        }, RuntimeException.class, CommandLineTransformer.JVM_OPTION_PREFIX +
            " JVM parameters for Ignite batch scripts " +
            "with double quotes are not supported. " +
            "Use JVM_OPTS environment variable to pass any custom JVM option.");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return CommandLineTransformer.transform("-J-Xmx1g", "-J-XX:OnOutOfMemoryError=\"dir c:\\\"");
            }
        }, RuntimeException.class, CommandLineTransformer.JVM_OPTION_PREFIX +
            " JVM parameters for Ignite batch scripts " +
            "with double quotes are not supported. " +
            "Use JVM_OPTS environment variable to pass any custom JVM option.");
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformIfSeveralArgumentsWithoutDashPrefix() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return CommandLineTransformer.transform("c:\\qw.xml", "abc", "d");
            }
        }, RuntimeException.class, "Unrecognised parameter has been found: abc");
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformIfOnlyPathToConfigSpecified() throws Exception {
        assertEquals(
            "\"INTERACTIVE=0\" \"QUIET=-DIGNITE_QUIET=true\" \"NO_PAUSE=0\" \"NO_JMX=0\" " +
            "\"JVM_XOPTS=\" \"CONFIG=c:\\qw.xml\"",
            CommandLineTransformer.transform("c:\\qw.xml"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformIfAllSupportedArguments() throws Exception {
        assertEquals(
            "\"INTERACTIVE=1\" \"QUIET=-DIGNITE_QUIET=false\" \"NO_PAUSE=1\" \"NO_JMX=1\" " +
                "\"JVM_XOPTS=-Xmx1g -Xms1m\" " +
                "\"CONFIG=\"c:\\path to\\русский каталог\"\"",
            CommandLineTransformer.transform("-i", "-np", "-v", "-J-Xmx1g", "-J-Xms1m", "-nojmx",
                "\"c:\\path to\\русский каталог\""));
    }
}