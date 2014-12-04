/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.startup;

import org.apache.commons.cli.*;
import org.apache.ignite.configuration.*;
import org.apache.log4j.*;
import org.apache.log4j.varia.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.logger.*;
import org.jetbrains.annotations.*;
import org.springframework.beans.*;
import org.springframework.context.*;
import org.springframework.context.support.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.IgniteState.*;

/**
 * This class defines random command-line GridGain loader. This loader can be used
 * to randomly start and stop GridGain from command line for tests. This loader is a Java
 * application with {@link #main(String[])} method that accepts command line arguments.
 * See below for details.
 */
public final class GridRandomCommandLineLoader {
    /** Name of the system property defining name of command line program. */
    private static final String GRIDGAIN_PROG_NAME = "GRIDGAIN_PROG_NAME";

    /** Copyright text. Ant processed. */
    private static final String COPYRIGHT = "Copyright (C) 2014 GridGain Systems.";

    /** Version. Ant processed. */
    private static final String VER = "x.x.x";

    /** */
    private static final String OPTION_HELP = "help";

    /** */
    private static final String OPTION_CFG = "cfg";

    /** */
    private static final String OPTION_MIN_TTL = "minTtl";

    /** */
    private static final String OPTION_MAX_TTL = "maxTtl";

    /** */
    private static final String OPTION_DURATION = "duration";

    /** */
    private static final String OPTION_LOG_CFG = "logCfg";

    /** Minimal value for timeout in milliseconds. */
    private static final long DFLT_MIN_TIMEOUT = 1000;

    /** Maximum value for timeout in milliseconds. */
    private static final long DFLT_MAX_TIMEOUT = 1000 * 20;

    /** Work timeout in milliseconds. */
    private static final long DFLT_RUN_TIMEOUT = 1000 * 60 * 5;

    /** Latch. */
    private static CountDownLatch latch;

    /**
     * Enforces singleton.
     */
    private GridRandomCommandLineLoader() {
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
    private static void echo(GridException e) {
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
    private static void exit(@Nullable String errMsg, @Nullable Options options, int exitCode) {
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
     * Prints logo.
     */
    private static void logo() {
        echo("GridGain Random Command Line Loader, ver. " + VER);
        echo(COPYRIGHT);
        echo("");
    }

    /**
     * Main entry point.
     *
     * @param args Command line arguments.
     */
    @SuppressWarnings({"BusyWait"})
    public static void main(String[] args) {
        System.setProperty(GridSystemProperties.GG_UPDATE_NOTIFIER, "false");

        logo();

        Options options = createOptions();

        // Create the command line parser.
        CommandLineParser parser = new PosixParser();

        String cfgPath = null;

        long minTtl = DFLT_MIN_TIMEOUT;
        long maxTtl = DFLT_MAX_TIMEOUT;
        long duration = DFLT_RUN_TIMEOUT;

        String logCfgPath = null;

        try {
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption(OPTION_HELP))
                exit(null, options, 0);

            if (!cmd.hasOption(OPTION_LOG_CFG))
                exit("-log should be set", options, -1);
            else
                logCfgPath = cmd.getOptionValue(OPTION_LOG_CFG);

            if (cmd.hasOption(OPTION_CFG))
                cfgPath = cmd.getOptionValue(OPTION_CFG);

            try {
                if (cmd.hasOption(OPTION_DURATION))
                    duration = Long.parseLong(cmd.getOptionValue(OPTION_DURATION));
            }
            catch (NumberFormatException ignored) {
                exit("Invalid argument for option: " + OPTION_DURATION, options, -1);
            }

            try {
                if (cmd.hasOption(OPTION_MIN_TTL))
                    minTtl = Long.parseLong(cmd.getOptionValue(OPTION_MIN_TTL));
            }
            catch (NumberFormatException ignored) {
                exit("Invalid argument for option: " + OPTION_MIN_TTL, options, -1);
            }

            try {
                if (cmd.hasOption(OPTION_MAX_TTL))
                    maxTtl = Long.parseLong(cmd.getOptionValue(OPTION_MAX_TTL));
            }
            catch (NumberFormatException ignored) {
                exit("Invalid argument for option: " + OPTION_MAX_TTL, options, -1);
            }

            if (minTtl >= maxTtl)
                exit("Invalid arguments for options: " + OPTION_MAX_TTL + ", " + OPTION_MIN_TTL, options, -1);
        }
        catch (ParseException e) {
            exit(e.getMessage(), options, -1);
        }

        System.out.println("Configuration path: " + cfgPath);
        System.out.println("Log4j configuration path: " + logCfgPath);
        System.out.println("Duration: " + duration);
        System.out.println("Minimum TTL: " + minTtl);
        System.out.println("Maximum TTL: " + maxTtl);

        G.addListener(new GridGainListener() {
            @Override public void onStateChange(String name, IgniteState state) {
                if (state == STOPPED && latch != null)
                    latch.countDown();
            }
        });

        Random rand = new Random();

        long now = System.currentTimeMillis();

        long end = duration + System.currentTimeMillis();

        try {
            while (now < end) {
                G.start(getConfiguration(cfgPath, logCfgPath));

                long delay = rand.nextInt((int)(maxTtl - minTtl)) + minTtl;

                delay = (now + delay > end) ? (end - now) : delay;

                now = System.currentTimeMillis();

                echo("Time left (ms): " + (end - now));

                echo("Going to sleep for (ms): " + delay);

                Thread.sleep(delay);

                G.stopAll(false);

                now = System.currentTimeMillis();
            }
        }
        catch (GridException e) {
            echo(e);

            exit("Failed to start grid: " + e.getMessage(), null, -1);
        }
        catch (InterruptedException e) {
            echo("Loader was interrupted (exiting): " + e.getMessage());
        }

        latch = new CountDownLatch(G.allGrids().size());

        try {
            while (latch.getCount() > 0)
                latch.await();
        }
        catch (InterruptedException e) {
            echo("Loader was interrupted (exiting): " + e.getMessage());
        }

        System.exit(0);
    }

    /**
     * Initializes configurations.
     *
     * @param springCfgPath Configuration file path.
     * @param logCfgPath Log file name.
     * @return List of configurations.
     * @throws GridException If an error occurs.
     */
    @SuppressWarnings("unchecked")
    private static IgniteConfiguration getConfiguration(String springCfgPath, @Nullable String logCfgPath)
        throws GridException {
        assert springCfgPath != null;

        File path = GridTestUtils.resolveGridGainPath(springCfgPath);

        if (path == null)
            throw new GridException("Spring XML configuration file path is invalid: " + new File(springCfgPath) +
                ". Note that this path should be either absolute path or a relative path to GRIDGAIN_HOME.");

        if (!path.isFile())
            throw new GridException("Provided file path is not a file: " + path);

        // Add no-op logger to remove no-appender warning.
        Appender app = new NullAppender();

        Logger.getRootLogger().addAppender(app);

        ApplicationContext springCtx;

        try {
            springCtx = new FileSystemXmlApplicationContext(path.toURI().toURL().toString());
        }
        catch (BeansException | MalformedURLException e) {
            throw new GridException("Failed to instantiate Spring XML application context: " + e.getMessage(), e);
        }

        Map cfgMap;

        try {
            // Note: Spring is not generics-friendly.
            cfgMap = springCtx.getBeansOfType(IgniteConfiguration.class);
        }
        catch (BeansException e) {
            throw new GridException("Failed to instantiate bean [type=" + IgniteConfiguration.class + ", err=" +
                e.getMessage() + ']', e);
        }

        if (cfgMap == null)
            throw new GridException("Failed to find a single grid factory configuration in: " + path);

        // Remove previously added no-op logger.
        Logger.getRootLogger().removeAppender(app);

        if (cfgMap.size() != 1)
            throw new GridException("Spring configuration file should contain exactly 1 grid configuration: " + path);

        IgniteConfiguration cfg = (IgniteConfiguration)F.first(cfgMap.values());

        assert cfg != null;

        if (logCfgPath != null)
            cfg.setGridLogger(new GridTestLog4jLogger(U.resolveGridGainUrl(logCfgPath)));

        return cfg;
    }

    /**
     * Creates cli options.
     *
     * @return Command line options
     */
    private static Options createOptions() {
        Options options = new Options();

        Option help = new Option(OPTION_HELP, "print this message");

        Option cfg = new Option(null, OPTION_CFG, true, "path to Spring XML configuration file.");

        cfg.setValueSeparator('=');
        cfg.setType(String.class);

        Option minTtl = new Option(null, OPTION_MIN_TTL, true, "node minimum time to live.");

        minTtl.setValueSeparator('=');
        minTtl.setType(Long.class);

        Option maxTtl = new Option(null, OPTION_MAX_TTL, true, "node maximum time to live.");

        maxTtl.setValueSeparator('=');
        maxTtl.setType(Long.class);

        Option duration = new Option(null, OPTION_DURATION, true, "run timeout.");

        duration.setValueSeparator('=');
        duration.setType(Long.class);

        Option log = new Option(null, OPTION_LOG_CFG, true, "path to log4j configuration file.");

        log.setValueSeparator('=');
        log.setType(String.class);

        options.addOption(help);

        OptionGroup grp = new OptionGroup();

        grp.setRequired(true);

        grp.addOption(cfg);
        grp.addOption(minTtl);
        grp.addOption(maxTtl);
        grp.addOption(duration);
        grp.addOption(log);

        options.addOptionGroup(grp);

        return options;
    }
}
