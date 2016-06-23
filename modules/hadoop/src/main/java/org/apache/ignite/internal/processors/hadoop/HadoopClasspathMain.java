package org.apache.ignite.internal.processors.hadoop;

import java.util.List;

import static java.lang.System.out;
import static java.lang.System.err;

/**
 * Main class to compose Hadoop classpath depending on the environment.
 * This class is designed to be independent on any Ignite classes as possible.
 * Please make sure to pass the path separator character as the 1st parameter to the main method.
 */
public class HadoopClasspathMain {
    /**
     * Main method to be executed from scripts. It prints the classpath to the standard output.
     *
     * @param args The 1st argument should be the path separator character (":" on Linux, ";" on Windows).
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            err.println("Path separator must be passed as the 1st argument.");

            System.exit(1);
        }

        final String sep = args[0];

        List<String> cp = HadoopClasspathUtils.getAsProcessClasspath();

        for (String s: cp) {
            if (s != null && s.length() > 0) {
                out.print(s);
                out.print(sep);
            }
        }

        out.println();
    }
}
