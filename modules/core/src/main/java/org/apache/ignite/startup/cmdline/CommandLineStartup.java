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

import org.apache.ignite.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.swing.*;
import java.awt.*;
import java.io.*;
import java.lang.reflect.*;
import java.lang.reflect.Proxy;
import java.net.*;
import java.text.*;
import java.util.*;
import java.util.List;
import java.util.concurrent.*;

import static org.apache.ignite.IgniteState.*;
import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.internal.IgniteVersionUtils.*;

/**
 * This class defines command-line Ignite startup. This startup can be used to start Ignite
 * outside of any hosting environment from command line. This startup is a Java application with
 * {@link #main(String[])} method that accepts command line arguments. It accepts just one
 * parameter which is Spring XML configuration file path. You can run this class from command
 * line without parameters to get help message.
 * <p>
 * Note that scripts {@code ${IGNITE_HOME}/bin/ignite.{sh|bat}} shipped with Ignite use
 * this startup and you can use them as an example.
 */
@SuppressWarnings({"CallToSystemExit"})
public final class CommandLineStartup {
    /** Quite log flag. */
    private static final boolean QUITE;

    /** Build date. */
    private static Date releaseDate;

    /**
     * Static initializer.
     */
    static {
        String quiteStr = System.getProperty(IgniteSystemProperties.IGNITE_QUIET);

        boolean quite = true;

        if (quiteStr != null) {
            quiteStr = quiteStr.trim();

            if (!quiteStr.isEmpty()) {
                if ("false".equalsIgnoreCase(quiteStr))
                    quite = false;
                else if (!"true".equalsIgnoreCase(quiteStr)) {
                    System.err.println("Invalid value for '" + IgniteSystemProperties.IGNITE_QUIET +
                        "' VM parameter (must be {true|false}): " + quiteStr);

                    quite = false;
                }
            }
        }

        QUITE = quite;

        // Mac OS specific customizations: app icon and about dialog.
        try {
            releaseDate = new SimpleDateFormat("ddMMyyyy", Locale.US).parse(RELEASE_DATE_STR);

            Class<?> appCls = Class.forName("com.apple.eawt.Application");

            Object osxApp = appCls.getDeclaredMethod("getApplication").invoke(null);

            String icoPath = "ggcube_node_128x128.png";

            URL url = CommandLineStartup.class.getResource(icoPath);

            assert url != null : "Unknown icon path: " + icoPath;

            ImageIcon ico = new ImageIcon(url);

            appCls.getDeclaredMethod("setDockIconImage", Image.class).invoke(osxApp, ico.getImage());

            // Setting Up about dialog
            Class<?> aboutHndCls = Class.forName("com.apple.eawt.AboutHandler");

            final URL bannerUrl = CommandLineStartup.class.getResource("ggcube_node_48x48.png");

            Object aboutHndProxy = Proxy.newProxyInstance(
                appCls.getClassLoader(),
                new Class<?>[] {aboutHndCls},
                new InvocationHandler() {
                    @Override public Object invoke(Object proxy, Method mtd, Object[] args) throws Throwable {
                        AboutDialog.centerShow("Ignite Node", bannerUrl.toExternalForm(), VER_STR,
                            releaseDate, COPYRIGHT);

                        return null;
                    }
                }
            );

            appCls.getDeclaredMethod("setAboutHandler", aboutHndCls).invoke(osxApp, aboutHndProxy);
        }
        catch (Throwable ignore) {
            // Ignore.
        }
    }

    /**
     * Enforces singleton.
     */
    private CommandLineStartup() {
        // No-op.
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

        String runner = System.getProperty(IGNITE_PROG_NAME, "ignite.{sh|bat}");

        int space = runner.indexOf(' ');

        runner = runner.substring(0, space == -1 ? runner.length() : space);

        if (showUsage) {
            boolean ignite = runner.contains("ignite.");

            X.error(
                "Usage:",
                "    " + runner + (ignite ? " [?]|[path {-v}]|[-i]" : " [?]|[-v]"),
                "    Where:",
                "    ?, /help, -help - show this message.",
                "    -v              - verbose mode (quiet by default).");

            if (ignite) {
                X.error(
                    "    -i              - interactive mode (choose configuration file from list).",
                    "    path            - path to Spring XML configuration file.",
                    "                      Path can be absolute or relative to IGNITE_HOME.",
                    " ",
                    "Spring file should contain one bean definition of Java type",
                    "'org.apache.ignite.configuration.IgniteConfiguration'. Note that bean will be",
                    "fetched by the type and its ID is not used.");
            }
        }

        System.exit(exitCode);
    }

    /**
     * Tests whether argument is help argument.
     *
     * @param arg Command line argument.
     * @return {@code true} if given argument is a help argument, {@code false} otherwise.
     */
    @SuppressWarnings({"IfMayBeConditional"})
    private static boolean isHelp(String arg) {
        String s;

        if (arg.startsWith("--"))
            s = arg.substring(2);
        else if (arg.startsWith("-") || arg.startsWith("/") || arg.startsWith("\\"))
            s = arg.substring(1);
        else
            s = arg;

        return "?".equals(s) || "help".equalsIgnoreCase(s) || "h".equalsIgnoreCase(s);
    }

    /**
     * Interactively asks for configuration file path.
     *
     * @return Configuration file path. {@code null} if operation  was cancelled.
     * @throws IOException In case of error.
     */
    @Nullable private static String askConfigFile() throws IOException {
        List<GridTuple3<String, Long, File>> files = GridConfigurationFinder.getConfigFiles();

        String title = "Available configuration files:";

        X.println(title);
        X.println(U.dash(title.length()));

        for (int i = 0; i < files.size(); i++)
            System.out.println(i + ":\t" + files.get(i).get1());

        X.print("\nChoose configuration file ('c' to cancel) [0]: ");

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        String line = reader.readLine();

        if ("c".equalsIgnoreCase(line)) {
            System.out.println("\nOperation cancelled.");

            return null;
        }

        if (line != null && line.isEmpty())
            line = "0";

        try {
            GridTuple3<String, Long, File> file = files.get(Integer.valueOf(line));

            X.println("\nUsing configuration: " + file.get1() + "\n");

            return file.get3().getAbsolutePath();
        }
        catch (Exception ignored) {
            X.error("\nInvalid selection: " + line);

            return null;
        }
    }

    /**
     * Main entry point.
     *
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        if (!QUITE) {
            X.println("Ignite Command Line Startup, ver. " + ACK_VER_STR);
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

        String cfg = null;

        if (args.length > 0)
            cfg = args[0];
        else {
            try {
                cfg = askConfigFile();

                if (cfg == null)
                    exit(null, false, 0);
            }
            catch (IOException e) {
                exit("Failed to run interactive mode: " + e.getMessage(), false, -1);
            }
        }

        // Name of the grid loaded from the command line (unique in JVM).
        final String gridName;

        try {
            gridName = G.start(cfg).name();
        }
        catch (Throwable e) {
            e.printStackTrace();

            String note = "";

            if (X.hasCause(e, ClassNotFoundException.class))
                note = "\nNote! You may use 'USER_LIBS' environment variable to specify your classpath.";

            exit("Failed to start grid: " + e.getMessage() + note, false, -1);

            return;
        }

        // Exit latch for the grid loaded from the command line.
        final CountDownLatch latch = new CountDownLatch(1);

        G.addListener(new IgnitionListener() {
            @Override public void onStateChange(String name, IgniteState state) {
                // Skip all grids except loaded from the command line.
                if (!F.eq(gridName, name))
                    return;

                if (state == STOPPED || state == STOPPED_ON_SEGMENTATION)
                    latch.countDown();
            }
        });

        try {
            latch.await();
        }
        catch (InterruptedException e) {
            X.error("Start was interrupted (exiting): " + e.getMessage());
        }

        String code = System.getProperty(IGNITE_RESTART_CODE);

        if (code != null)
            try {
                System.exit(Integer.parseInt(code));
            }
            catch (NumberFormatException ignore) {
                System.exit(0);
            }
        else
            System.exit(0);
    }
}
