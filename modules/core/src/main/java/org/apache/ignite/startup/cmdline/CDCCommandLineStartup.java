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

import java.net.URL;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cdc.ChangeDataCaptureConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.cdc.IgniteCDC;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.spring.IgniteSpringHelper;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_NO_SHUTDOWN_HOOK;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PROG_NAME;
import static org.apache.ignite.internal.IgniteComponentType.SPRING;
import static org.apache.ignite.internal.IgniteVersionUtils.ACK_VER_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;
import static org.apache.ignite.startup.cmdline.CommandLineStartup.isHelp;

/**
 * This class defines command-line Ignite Capture Data Change startup. This startup can be used to start Ignite
 * Capture Data Change application outside of any hosting environment from command line.
 * This startup is a Java application with {@link #main(String[])} method that accepts command line arguments.
 * It accepts just one parameter which is Spring XML configuration file path.
 * You can run this class from command line without parameters to get help message.
 * <p>
 * Note that scripts {@code ${IGNITE_HOME}/bin/cdc.{sh|bat}} shipped with Ignite use
 * this startup and you can use them as an example.
 * <p>
 *
 * @see IgniteCDC
 */
public class CDCCommandLineStartup {
    /** Quite log flag. */
    private static final boolean QUITE = IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_QUIET);

    /**
     * Main entry point.
     *
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        if (!QUITE) {
            X.println("Ignite CDC Command Line Startup, ver. " + ACK_VER_STR);
            X.println(COPYRIGHT);
            X.println();
        }

        if (args.length > 1)
            exit("Too many arguments.", true, -1);

        if (args.length > 0 && isHelp(args[0]))
            exit(null, true, 0);

        if (args.length > 0 && args[0].isEmpty())
            exit("Empty argument.", true, 1);

        if (args.length > 0 && args[0].charAt(0) == '-')
            exit("Invalid arguments: " + args[0], true, -1);

        try {
            URL cfgUrl = U.resolveSpringUrl(args[0]);

            IgniteSpringHelper spring = SPRING.create(false);

            IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext> cfgTuple =
                spring.loadConfigurations(cfgUrl);

            if (cfgTuple.get1().size() > 1)
                exit("Found " + cfgTuple.get1().size() + " configurations. Can use only 1", false, 1);

            IgniteConfiguration cfg = cfgTuple.get1().iterator().next();

            IgniteCDC cdc = new IgniteCDC(cfg, consumerConfig(cfgUrl, spring));

            if (!IgniteSystemProperties.getBoolean(IGNITE_NO_SHUTDOWN_HOOK, false)) {
                Runtime.getRuntime().addShutdownHook(new Thread("cdc-shutdown-hook") {
                    @Override public void run() {
                        cdc.stop();
                    }
                });
            }

            Thread appThread = new Thread(cdc);

            appThread.start();

            appThread.join();
        }
        catch (Throwable e) {
            e.printStackTrace();

            String note = "";

            if (X.hasCause(e, ClassNotFoundException.class))
                note = "\nNote! You may use 'USER_LIBS' environment variable to specify your classpath.";

            exit("Failed to run CDC: " + e.getMessage() + note, false, -1);
        }
    }

    /**
     * @param cfgUrl String configuration URL.
     * @param spring Ignite spring helper.
     * @return CDC consumer defined in spring configuration.
     * @throws IgniteCheckedException
     */
    private static ChangeDataCaptureConfiguration consumerConfig(URL cfgUrl, IgniteSpringHelper spring) throws IgniteCheckedException {
        Map<Class<?>, Object> cdcCfgs = spring.loadBeans(cfgUrl, ChangeDataCaptureConfiguration.class);

        if (cdcCfgs == null || cdcCfgs.size() != 1)
            exit("Exact 1 CaptureDataChangeConfiguration configuration should be defined", false, 1);

        return (ChangeDataCaptureConfiguration)cdcCfgs.values().iterator().next();
    }

    /**
     * Exists with optional error message, usage show and exit code.
     *
     * @param errMsg Optional error message.
     * @param showUsage Whether or not to show usage information.
     * @param exitCode Exit code.
     */
    private static void exit(@Nullable String errMsg, boolean showUsage, int exitCode) {
        if (errMsg != null)
            X.error(errMsg);

        String runner = System.getProperty(IGNITE_PROG_NAME, "ignite-cdc.{sh|bat}");

        int space = runner.indexOf(' ');

        runner = runner.substring(0, space == -1 ? runner.length() : space);

        if (showUsage) {
            X.error(
                "Usage:",
                "    " + runner + " [?]|[path]",
                "    Where:",
                "    ?, /help, -help, - show this message.",
                "    -v               - verbose mode (quiet by default).",
                "    path            - path to Spring XML configuration file.",
                "                      Path can be absolute or relative to IGNITE_HOME.",
                " ",
                "Spring file should contain bean definition of 'org.apache.ignite.configuration.IgniteConfiguration' " +
                "And one or more implementations of 'org.apache.ignite.cdc.CDCConsumer'." +
                "Note that bean will be fetched by the type and its ID is not used.");
        }

        System.exit(exitCode);
    }
}
