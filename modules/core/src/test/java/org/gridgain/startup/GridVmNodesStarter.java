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

package org.gridgain.startup;

import org.apache.commons.cli.*;
import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.log4j.*;
import org.apache.log4j.varia.*;
import org.apache.ignite.internal.util.typedef.*;
import org.gridgain.testframework.*;
import org.springframework.beans.*;
import org.springframework.context.*;
import org.springframework.context.support.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.IgniteSystemProperties.*;

/**
 * This class
 *
 */
public final class GridVmNodesStarter {
    /** Name of the system property defining name of command line program. */
    private static final String GRIDGAIN_PROG_NAME = "GRIDGAIN_PROG_NAME";

    /** */
    private static final String GRID_NAME_PREF = "gg-vm-grid-";

    /** */
    private static final int DFLT_NODES_COUNT = 20;

    /** */
    private static final String OPTION_CFG = "cfg";

    /** */
    private static final String OPTION_N = "n";

    /** */
    private static final AtomicInteger gridCnt = new AtomicInteger();

    /**
     * Enforces singleton.
     */
    private GridVmNodesStarter() {
        // No-op.
    }

    /**
     * Echos the given messages.
     *
     * @param msg Message to echo.
     */
    private static void echo(String msg) {
        assert msg != null;

        System.out.println(msg);
    }

    /**
     * Echos exception stack trace.
     *
     * @param e Exception to print.
     */
    private static void echo(IgniteCheckedException e) {
        assert e != null;

        System.err.println(e);
    }

    /**
     * Exists with optional error message, usage show and exit code.
     *
     * @param errMsg Optional error message.
     * @param options Command line options to show usage information.
     * @param exitCode Exit code.
     */
    private static void exit(String errMsg, Options options, int exitCode) {
        if (errMsg != null)
            echo("ERROR: " + errMsg);

        String runner = System.getProperty(GRIDGAIN_PROG_NAME, "randggstart.{sh|bat}");

        int space = runner.indexOf(' ');

        runner = runner.substring(0, space == -1 ? runner.length() : space);

        if (options != null) {
            HelpFormatter formatter = new HelpFormatter();

            formatter.printHelp(runner, options);
        }

        System.exit(exitCode);
    }

    /**
     * Main entry point.
     *
     * @param args Command line arguments.
     * @throws IgniteCheckedException If failed.
     */
    public static void main(String[] args) throws IgniteCheckedException {
        System.setProperty(GG_UPDATE_NOTIFIER, "false");

        Options options = createOptions();

        // Create the command line parser.
        CommandLineParser parser = new PosixParser();

        String cfgPath = null;

        Integer nodesCnt = null;

        try {
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption(OPTION_CFG))
                cfgPath = cmd.getOptionValue(OPTION_CFG);

            if (cmd.hasOption(OPTION_N))
                try {
                    nodesCnt = Integer.parseInt(cmd.getOptionValue(OPTION_N));
                }
                catch (NumberFormatException ignored) {
                    // No-op.
                }

            if (nodesCnt == null)
                nodesCnt = DFLT_NODES_COUNT;
        }
        catch (ParseException e) {
            exit(e.getMessage(), options, -1);
        }

        System.out.println();
        System.out.println(">>> VM Nodes Starter parameters:");
        System.out.println("  Nodes Count: " + nodesCnt);
        System.out.println("  Config Path: " + cfgPath);
        System.out.println();

        final IgniteConfiguration[] cfgs = new IgniteConfiguration[nodesCnt];

        for (int i = 0; i < nodesCnt; i++)
            cfgs[i] = getConfigurations(cfgPath).iterator().next();

        final AtomicInteger cfgIdx = new AtomicInteger(0);

        GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                G.start(cfgs[cfgIdx.getAndIncrement()]);

                return null;
            }
        }, nodesCnt, "test-node-starter");
    }

    /**
     * Initializes configurations.
     *
     *
     * @param springCfgPath Configuration file path.
     * @return List of configurations.
     * @throws IgniteCheckedException If an error occurs.
     */
    @SuppressWarnings("unchecked")
    private static Iterable<IgniteConfiguration> getConfigurations(String springCfgPath)
        throws IgniteCheckedException {
        File path = GridTestUtils.resolveGridGainPath(springCfgPath);

        if (path == null)
            throw new IgniteCheckedException("Spring XML configuration file path is invalid: " + new File(springCfgPath) +
                ". Note that this path should be either absolute path or a relative path to GRIDGAIN_HOME.");

        if (!path.isFile())
            throw new IgniteCheckedException("Provided file path is not a file: " + path);

        // Add no-op logger to remove no-appender warning.
        Appender app = new NullAppender();

        Logger.getRootLogger().addAppender(app);

        ApplicationContext springCtx;

        try {
            springCtx = new FileSystemXmlApplicationContext(path.toURI().toURL().toString());
        }
        catch (BeansException | MalformedURLException e) {
            throw new IgniteCheckedException("Failed to instantiate Spring XML application context: " + e.getMessage(), e);
        }

        Map cfgMap;

        try {
            // Note: Spring is not generics-friendly.
            cfgMap = springCtx.getBeansOfType(IgniteConfiguration.class);
        }
        catch (BeansException e) {
            throw new IgniteCheckedException("Failed to instantiate bean [type=" + IgniteConfiguration.class + ", err=" +
                e.getMessage() + ']', e);
        }

        if (cfgMap == null)
            throw new IgniteCheckedException("Failed to find a single grid factory configuration in: " + path);

        // Remove previously added no-op logger.
        Logger.getRootLogger().removeAppender(app);

        if (cfgMap.isEmpty())
            throw new IgniteCheckedException("Can't find grid factory configuration in: " + path);

        Collection<IgniteConfiguration> res = new ArrayList<>();

        for (IgniteConfiguration cfg : (Collection<IgniteConfiguration>)cfgMap.values()) {
            res.add(cfg);

            cfg.setGridName(GRID_NAME_PREF + gridCnt.incrementAndGet());
        }

        return res;
    }

    /**
     * Creates cli options.
     *
     * @return Command line options
     */
    private static Options createOptions() {
        Options options = new Options();

        OptionGroup grp = new OptionGroup();

        grp.setRequired(true);

        Option cfg = new Option(OPTION_CFG, null, true, "path to Spring XML configuration file.");

        cfg.setArgName("file");

        Option n = new Option(null, OPTION_N, true, "nodes count.");

        n.setValueSeparator('=');
        n.setType(Integer.class);

        grp.addOption(cfg);
        grp.addOption(n);

        options.addOptionGroup(grp);

        return options;
    }
}
