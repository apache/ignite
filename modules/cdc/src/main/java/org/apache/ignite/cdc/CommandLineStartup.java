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

package org.apache.ignite.cdc;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.spring.IgniteSpringHelper;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

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
 * Note that scripts {@code ${IGNITE_HOME}/bin/ignite.{sh|bat}} shipped with Ignite use
 * this startup and you can use them as an example.
 */
public class CommandLineStartup {
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

            IgniteConfiguration igniteCfg =  cfgTuple.get1().iterator().next();

            Map<Class<?>, Object> consumersMap = spring.loadBeans(cfgUrl, CDCConsumer.class);

            if (consumersMap.isEmpty())
                exit("No CDC consumers defined", false, 1);

            List<CDCConsumer> consumers = new ArrayList<>();

            for (Object c : consumersMap.values()) {
                if (c instanceof CDCConsumer)
                    consumers.add((CDCConsumer)c);
                else if (c instanceof Collection)
                    consumers.addAll((Collection<CDCConsumer>)c);
                else
                    throw new IllegalArgumentException("Unexpected bean type " + c.getClass());
            }

            initEnvironment(igniteCfg);

            IgniteCDC app = new IgniteCDC(igniteCfg, consumers);

            app.run();
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
     * @param cfg
     * @throws IgniteCheckedException
     */
    private static void initEnvironment(IgniteConfiguration cfg) throws IgniteCheckedException {
        String igniteHome = cfg.getIgniteHome();

        // Set Ignite home.
        if (igniteHome == null)
            igniteHome = U.getIgniteHome();
        else
            // If user provided IGNITE_HOME - set it as a system property.
            U.setIgniteHome(igniteHome);

        String userProvidedWorkDir = cfg.getWorkDirectory();

        // Correctly resolve work directory and set it back to configuration.
        String workDir = U.workDirectory(userProvidedWorkDir, igniteHome);

        cfg.setWorkDirectory(workDir);

        UUID appLogId = UUID.randomUUID();
        IgniteLogger cfgLog = IgnitionEx.IgniteNamedInstance.initLogger(cfg.getGridLogger(), appLogId, workDir);

        cfg.setGridLogger(cfgLog);
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

        String runner = System.getProperty(IGNITE_PROG_NAME, "ignite-cdc");

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
