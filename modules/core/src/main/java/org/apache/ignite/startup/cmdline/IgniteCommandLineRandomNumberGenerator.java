/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.startup.cmdline;

import java.util.*;

/**
 * Generates a random number and prints it to the console.
 */
public class IgniteCommandLineRandomNumberGenerator {
    /**
     * Main method.
     *
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        System.out.println(UUID.randomUUID());
    }
}
