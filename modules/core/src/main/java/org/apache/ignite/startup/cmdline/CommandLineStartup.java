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

import java.awt.Image;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import javax.swing.ImageIcon;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteState;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.IgnitionListener;
import org.apache.ignite.SystemProperty;
import org.apache.ignite.internal.binary.BinaryArray;
import org.apache.ignite.internal.processors.cache.ExchangeContext;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.PartitionsEvictManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.freelist.PagesList;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryEventBuffer;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryHandler;
import org.apache.ignite.internal.util.GridConfigurationFinder;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.internal.TcpCommunicationConfiguration;
import org.apache.ignite.spi.deployment.local.LocalDeploymentSpi;
import org.jetbrains.annotations.Nullable;

import static java.lang.String.format;
import static org.apache.ignite.IgniteState.STARTED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PROG_NAME;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_RESTART_CODE;
import static org.apache.ignite.internal.IgniteVersionUtils.ACK_VER_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;
import static org.apache.ignite.internal.IgniteVersionUtils.RELEASE_DATE;
import static org.apache.ignite.internal.IgniteVersionUtils.VER_STR;

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

    /** Command to print Ignite system properties info. */
    static final String PRINT_PROPS_COMMAND = "-systemProps";

    /** Classes with Ignite system properties. */
    static final List<Class<?>> PROPS_CLS = new ArrayList<>(Arrays.asList(
        IgniteSystemProperties.class,
        ExchangeContext.class,
        GridCacheMapEntry.class,
        LocalDeploymentSpi.class,
        GridCacheDatabaseSharedManager.class,
        PartitionsEvictManager.class,
        PagesList.class,
        PagesList.PagesCache.class,
        GridCacheOffheapManager.class,
        CacheContinuousQueryEventBuffer.class,
        CacheContinuousQueryHandler.class,
        OffheapReadWriteLock.class,
        TcpCommunicationConfiguration.class,
        BinaryArray.class
    ));

    static {
        String h2TreeCls = "org.apache.ignite.internal.processors.query.h2.database.H2Tree";
        String zkDiscoImpl = "org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoveryImpl";
        String zkTcpDiscoIpFinder = "org.apache.ignite.spi.discovery.tcp.ipfinder.zk.TcpDiscoveryZookeeperIpFinder";
        String calciteQryProc = "org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor";

        try {
            if (U.inClassPath(h2TreeCls))
                PROPS_CLS.add(Class.forName(h2TreeCls));

            if (U.inClassPath(zkDiscoImpl)) {
                PROPS_CLS.add(Class.forName(zkDiscoImpl));
                PROPS_CLS.add(Class.forName(zkTcpDiscoIpFinder));
            }

            if (U.inClassPath(calciteQryProc))
                PROPS_CLS.add(Class.forName(calciteQryProc));
        }
        catch (ClassNotFoundException ignored) {
            // No-op.
        }
    }

    /** @see IgniteSystemProperties#IGNITE_PROG_NAME */
    public static final String DFLT_PROG_NAME = "ignite.{sh|bat}";

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
            Class<?> appCls = Class.forName("com.apple.eawt.Application");

            Object osxApp = appCls.getDeclaredMethod("getApplication").invoke(null);

            String icoPath = "logo_ignite_128x128.png";

            URL url = CommandLineStartup.class.getResource(icoPath);

            assert url != null : "Unknown icon path: " + icoPath;

            ImageIcon ico = new ImageIcon(url);

            appCls.getDeclaredMethod("setDockIconImage", Image.class).invoke(osxApp, ico.getImage());

            // Setting Up about dialog
            Class<?> aboutHndCls = Class.forName("com.apple.eawt.AboutHandler");

            final URL bannerUrl = CommandLineStartup.class.getResource("logo_ignite_48x48.png");

            Object aboutHndProxy = Proxy.newProxyInstance(
                appCls.getClassLoader(),
                new Class<?>[] {aboutHndCls},
                new InvocationHandler() {
                    @Override public Object invoke(Object proxy, Method mtd, Object[] args) throws Throwable {
                        AboutDialog.centerShow("Ignite Node", bannerUrl.toExternalForm(), VER_STR,
                            RELEASE_DATE, COPYRIGHT);

                        return null;
                    }
                }
            );

            appCls.getDeclaredMethod("setAboutHandler", aboutHndCls).invoke(osxApp, aboutHndProxy);
        }
        catch (Exception ignore) {
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

        String runner = System.getProperty(IGNITE_PROG_NAME, DFLT_PROG_NAME);

        int space = runner.indexOf(' ');

        runner = runner.substring(0, space == -1 ? runner.length() : space);

        if (showUsage) {
            boolean ignite = runner.contains("ignite.");

            X.error(
                "Usage:",
                "    " + runner + (ignite ? " [?]|[path {-v}{-np}]|[-i]" : " [?]|[-v]"),
                "    Where:",
                "    ?, /help, -help, - show this message.",
                "    -v               - verbose mode (quiet by default).",
                "    -np              - no pause on exit (pause by default)",
                "    " + PRINT_PROPS_COMMAND + "     - prints Ignite system properties info.");

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
    public static boolean isHelp(String arg) {
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

        if (args.length > 0 && PRINT_PROPS_COMMAND.equalsIgnoreCase(args[0])) {
            printSystemPropertiesInfo();

            exit(null, false, 0);
        }

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
        final String igniteInstanceName;

        try {
            igniteInstanceName = G.start(cfg).name();
        }
        catch (Throwable e) {
            e.printStackTrace();

            String note = "";

            if (X.hasCause(e, ClassNotFoundException.class))
                note = "\nNote! You may use 'USER_LIBS' environment variable to specify your classpath.";

            exit("Failed to start grid: " + e.getMessage() + note, false, -1);

            if (e instanceof Error)
                throw e;

            return;
        }

        // Exit latch for the grid loaded from the command line.
        final CountDownLatch latch = new CountDownLatch(1);

        G.addListener(new IgnitionListener() {
            @Override public void onStateChange(String name, IgniteState state) {
                // Skip all grids except loaded from the command line.
                if (!F.eq(igniteInstanceName, name))
                    return;

                if (state != STARTED)
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

    /** Prints properties info to console. */
    private static void printSystemPropertiesInfo() {
        Map<String, Field> props = new TreeMap<>();
        int maxLength = 0;

        for (Class<?> cls : PROPS_CLS) {
            for (Field field : cls.getFields()) {
                SystemProperty ann = field.getAnnotation(SystemProperty.class);

                if (ann != null) {
                    try {
                        String name = U.staticField(cls, field.getName());

                        maxLength = Math.max(maxLength, name.length());

                        props.put(name, field);
                    }
                    catch (IgniteCheckedException ignored) {
                        // No-op.
                    }
                }
            }
        }

        String fmt = "%-" + maxLength + "s - %s[%s] %s.%s";

        props.forEach((name, field) -> {
            String deprecated = field.isAnnotationPresent(Deprecated.class) ? "[Deprecated] " : "";

            SystemProperty prop = field.getAnnotation(SystemProperty.class);

            String defaults = prop.defaults();

            if (prop.type() == Boolean.class && defaults.isEmpty())
                defaults = " Default is false.";
            else if (!defaults.isEmpty())
                defaults = " Default is " + defaults + '.';

            X.println(format(fmt, name, deprecated, prop.type().getSimpleName(), prop.value(), defaults));
        });
    }
}
