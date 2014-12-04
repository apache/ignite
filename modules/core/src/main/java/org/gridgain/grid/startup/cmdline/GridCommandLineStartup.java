/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.startup.cmdline;

import org.gridgain.grid.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
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

import static org.gridgain.grid.IgniteState.*;
import static org.gridgain.grid.GridSystemProperties.*;
import static org.gridgain.grid.kernal.GridProductImpl.*;

/**
 * This class defines command-line GridGain startup. This startup can be used to start GridGain
 * outside of any hosting environment from command line. This startup is a Java application with
 * {@link #main(String[])} method that accepts command line arguments. It accepts just one
 * parameter which is Spring XML configuration file path. You can run this class from command
 * line without parameters to get help message.
 * <p>
 * Note that scripts {@code ${GRIDGAIN_HOME}/bin/ggstart.{sh|bat}} shipped with GridGain use
 * this startup and you can use them as an example.
 */
@SuppressWarnings({"CallToSystemExit"})
public final class GridCommandLineStartup {
    /** Quite log flag. */
    private static final boolean QUITE;

    /** Build date. */
    private static Date releaseDate;

    /**
     * Static initializer.
     */
    static {
        String quiteStr = System.getProperty(GridSystemProperties.GG_QUIET);

        boolean quite = true;

        if (quiteStr != null) {
            quiteStr = quiteStr.trim();

            if (!quiteStr.isEmpty()) {
                if ("false".equalsIgnoreCase(quiteStr))
                    quite = false;
                else if (!"true".equalsIgnoreCase(quiteStr)) {
                    System.err.println("Invalid value for '" + GridSystemProperties.GG_QUIET +
                        "' VM parameter (must be {true|false}): " + quiteStr);

                    quite = false;
                }
            }
        }

        QUITE = quite;

        // Mac OS specific customizations: app icon and about dialog.
        try {
            releaseDate = new SimpleDateFormat("ddMMyyyy", Locale.US).parse(RELEASE_DATE);

            Class<?> appCls = Class.forName("com.apple.eawt.Application");

            Object osxApp = appCls.getDeclaredMethod("getApplication").invoke(null);

            String icoPath = "ggcube_node_128x128.png";

            URL url = GridCommandLineStartup.class.getResource(icoPath);

            assert url != null : "Unknown icon path: " + icoPath;

            ImageIcon ico = new ImageIcon(url);

            appCls.getDeclaredMethod("setDockIconImage", Image.class).invoke(osxApp, ico.getImage());

            // Setting Up about dialog
            Class<?> aboutHndCls = Class.forName("com.apple.eawt.AboutHandler");

            final URL bannerUrl = GridCommandLineStartup.class.getResource("ggcube_node_48x48.png");

            Object aboutHndProxy = Proxy.newProxyInstance(
                appCls.getClassLoader(),
                new Class<?>[] {aboutHndCls},
                new InvocationHandler() {
                    @Override public Object invoke(Object proxy, Method mtd, Object[] args) throws Throwable {
                        GridAboutDialog.centerShow("GridGain Node", bannerUrl.toExternalForm(), VER,
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
    private GridCommandLineStartup() {
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

        String runner = System.getProperty(GG_PROG_NAME, "ggstart.{sh|bat}");

        int space = runner.indexOf(' ');

        runner = runner.substring(0, space == -1 ? runner.length() : space);

        if (showUsage) {
            boolean ggstart = runner.contains("ggstart.");

            X.error(
                "Usage:",
                "    " + runner + (ggstart ? " [?]|[path {-v}]|[-i]" : " [?]|[-v]"),
                "    Where:",
                "    ?, /help, -help - show this message.",
                "    -v              - verbose mode (quiet by default).");

            if (ggstart) {
                X.error(
                    "    -i              - interactive mode (choose configuration file from list).",
                    "    path            - path to Spring XML configuration file.",
                    "                      Path can be absolute or relative to GRIDGAIN_HOME.",
                    " ",
                    "Spring file should contain one bean definition of Java type",
                    "'org.gridgain.grid.GridConfiguration'. Note that bean will be",
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
            X.println("GridGain Command Line Startup, ver. " + ACK_VER);
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

        G.addListener(new GridGainListener() {
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

        String code = System.getProperty(GG_RESTART_CODE);

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
