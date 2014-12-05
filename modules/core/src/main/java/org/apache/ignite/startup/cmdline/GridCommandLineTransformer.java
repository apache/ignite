/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.startup.cmdline;

import java.io.*;
import java.util.*;

/**
 * This class is a workaround for a versatile problems with passing arguments to the GridGain Windows batch launcher
 * scripts. It transforms command line options passed to the *.bat launcher scripts into a reformatted, adapted for
 * Windows batch usage variables.
 * <p>
 * The idea behind the workaround is:<br>
 * 1. User runs ggstart.bat with some args.<br>
 * 2. ggstart.bat calls parseargs.bat with all that args.<br>
 * 3. parsearg.bat runs this class with all that args.<br>
 * 4. This class transforms the args and prints them out.<br>
 * 5. parseargs.bat splits the output of this class and sets each variable it found.<br>
 * 6. ggstart.bat uses environment variables as if they were defined by a user.
 * <p>
 *
 * @see <a href="http://jcommander.org/">JCommander command line parameters parsing library</a>
 */
@SuppressWarnings("FieldCanBeLocal")
public class GridCommandLineTransformer {
    /** Prefix for every custom JVM option. */
    static final String JVM_OPTION_PREFIX = "-J";

    /** Flag to check in step 5 of the Workaround before splitting the output. */
    private static final String TRANSFORMATION_FAILED_FLAG = "GridCommandLineTransformerError";

    /** Delimiter used in step 5 of the Workaround. */
    private static final String ARGS_DELIMITER = " ";

    /** Interactive mode. */
    private boolean interactive;

    /** Verbose mode. */
    private boolean verbose;

    /** No pause mode. */
    private boolean noPause;

    /** Supported parameter, parsed manually. */
    private String jvmOptions = "";

    /** Supported parameter, parsed manually. */
    private String springCfgPath = "";

    /**
     * Private constructor to promote usage of a factory method {@link #transform(String...)}.
     */
    private GridCommandLineTransformer() {
        // No-op
    }

    /**
     * Main method being triggered in step 3 of the Workaround.
     * <p>
     * Method prints out TRANSFORMATION_FAILED_FLAG if something went wrong.
     *
     * @param args Command line arguments passed by a user in step 1 of the Workaround.
     */
    public static void main(String[] args) {
        PrintStream ps = null;

        try {
            // Intentionality configure output stream with UTF-8 encoding to support  non-ASCII named parameter values.
            ps = new PrintStream(System.out, true, "UTF-8");

            ps.println(transform(args));
        }
        catch (Throwable t) {
            t.printStackTrace();

            if (ps != null)
                ps.println(TRANSFORMATION_FAILED_FLAG);
        }
    }

    /**
     * @param args Arguments.
     * @return Transformed arguments.
     */
    public static String transform(String... args) {
        assert args != null;

        return new GridCommandLineTransformer().doTransformation(args);
    }

    /**
     * Actually does arguments transformation.
     *
     * @param args Arguments.
     * @return Transformed arguments.
     */
    private String doTransformation(String[] args) {
        List<String> argsList = new ArrayList<>();

        for (String arg : args) {
            switch (arg) {
                case "-i":
                    interactive = true;

                    break;

                case "-v":
                    verbose = true;

                    break;

                case "-np":
                    noPause = true;

                    break;

                default:
                    argsList.add(arg);
            }
        }

        return reformatArguments(argsList);
    }

    /**
     * Transforms parsed arguments into a string.
     *
     * @param args Non-standard arguments.
     * @return Transformed arguments.
     */
    private String reformatArguments(List<String> args) {
        StringBuilder sb = new StringBuilder();

        addArgWithValue(sb, "INTERACTIVE", formatBooleanValue(interactive));
        addArgWithValue(sb, "QUIET", "-DGRIDGAIN_QUIET=" + !verbose);
        addArgWithValue(sb, "NO_PAUSE", formatBooleanValue(noPause));

        parseJvmOptionsAndSpringConfig(args);

        addArgWithValue(sb, "JVM_XOPTS", jvmOptions);
        addArgWithValue(sb, "CONFIG", springCfgPath);

        return sb.toString().trim();
    }

    /**
     * Formats boolean value into a string one.
     *
     * @param val Boolean value to be formatted.
     * @return 1 if val is true 0 otherwise.
     */
    private String formatBooleanValue(boolean val) {
        return String.valueOf(val ? 1 : 0);
    }

    /**
     * Transforms one argument.
     *
     * @param sb StringBuilder where transformation result will be written.
     * @param arg Argument's name.
     * @param val Argument's value.
     */
    private void addArgWithValue(StringBuilder sb, String arg, Object val) {
        sb.append("\"");
        sb.append(arg);
        sb.append("=");
        sb.append(val);
        sb.append("\"");
        sb.append(ARGS_DELIMITER);
    }

    /**
     * Manually parses non-trivial (from JCommander point of view) arguments.
     *
     * @param args Collection of unknown (from JCommander point of view) arguments.
     */
    private void parseJvmOptionsAndSpringConfig(Iterable<String> args) {
        for (String arg : args) {
            if (arg.startsWith(JVM_OPTION_PREFIX)) {
                String jvmOpt = arg.substring(JVM_OPTION_PREFIX.length());

                if (!checkJVMOptionIsSupported(jvmOpt))
                    throw new RuntimeException(JVM_OPTION_PREFIX + " JVM parameters for GridGain batch scripts " +
                        "with double quotes are not supported. " +
                        "Use JVM_OPTS environment variable to pass any custom JVM option.");

                jvmOptions = jvmOptions.isEmpty() ? jvmOpt : jvmOptions + " " + jvmOpt;
            }
            else {
                if (springCfgPath.isEmpty())
                    springCfgPath = arg;
                else
                    throw new RuntimeException("Unrecognised parameter has been found: " + arg);
            }
        }
    }

    /**
     * Check if a JVM option is supported. Unsupported options are those who
     * need double quotes in the declaration. It's a limitation of a current Workaround
     *
     * @param jvmOpt JVM option to check.
     * @return true if option is supported false otherwise.
     */
    private boolean checkJVMOptionIsSupported(String jvmOpt) {
        return !(jvmOpt.contains("-XX:OnError") || jvmOpt.contains("-XX:OnOutOfMemoryError"));
    }
}
