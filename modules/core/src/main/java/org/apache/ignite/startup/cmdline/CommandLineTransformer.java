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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 * This class is a workaround for a versatile problems with passing arguments to the Ignite Windows batch launcher
 * scripts. It transforms command line options passed to the *.bat launcher scripts into a reformatted, adapted for
 * Windows batch usage variables.
 * <p>
 * The idea behind the workaround is:<br>
 * 1. User runs ignite.bat with some args.<br>
 * 2. ignite.bat calls parseargs.bat with all that args.<br>
 * 3. parsearg.bat runs this class with all that args.<br>
 * 4. This class transforms the args and prints them out.<br>
 * 5. parseargs.bat splits the output of this class and sets each variable it found.<br>
 * 6. ignite.bat uses environment variables as if they were defined by a user.
 * <p>
 *
 * @see <a href="http://jcommander.org/">JCommander command line parameters parsing library</a>
 */
@SuppressWarnings("FieldCanBeLocal")
public class CommandLineTransformer {
    /** Prefix for every custom JVM option. */
    static final String JVM_OPTION_PREFIX = "-J";

    /** Flag to check in step 5 of the Workaround before splitting the output. */
    private static final String TRANSFORMATION_FAILED_FLAG = "CommandLineTransformerError";

    /** Delimiter used in step 5 of the Workaround. */
    private static final String ARGS_DELIMITER = " ";

    /** Interactive mode. */
    private boolean interactive;

    /** Verbose mode. */
    private boolean verbose;

    /** No pause mode. */
    private boolean noPause;

    /** No JMX mode. */
    private boolean noJMX;

    /** Supported parameter, parsed manually. */
    private String jvmOptions = "";

    /** Supported parameter, parsed manually. */
    private String springCfgPath = "";

    /**
     * Private constructor to promote usage of a factory method {@link #transform(String...)}.
     */
    private CommandLineTransformer() {
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

            if (t instanceof Error)
                throw (Error)t;
        }
    }

    /**
     * @param args Arguments.
     * @return Transformed arguments.
     */
    public static String transform(String... args) {
        assert args != null;

        return new CommandLineTransformer().doTransformation(args);
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

                case "-nojmx":
                    noJMX = true;

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
        addArgWithValue(sb, "QUIET", "-DIGNITE_QUIET=" + !verbose);
        addArgWithValue(sb, "NO_PAUSE", formatBooleanValue(noPause));
        addArgWithValue(sb, "NO_JMX", formatBooleanValue(noJMX));

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
                    throw new RuntimeException(JVM_OPTION_PREFIX + " JVM parameters for Ignite batch scripts " +
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