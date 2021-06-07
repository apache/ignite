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

package org.apache.ignite.app;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import org.apache.ignite.internal.app.IgnitionImpl;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * The main entry point for run new Ignite node from CLI toolchain.
 */
public class IgniteCliRunner {
    /** CLI usage message. */
    private static String usage = "IgniteCliRunner [--config conf] nodeName";

    /**
     * Main method for run new Ignite node.
     *
     * For CLI args info see {@link IgniteCliRunner#usage}
     *
     * @param args CLI args to start new node.
     * @throws IOException if any issues with reading config file.
     */
    public static void main(String[] args) throws IOException {
        Args parsedArgs = null;

        try {
            parsedArgs = Args.parseArgs(args);
        }
        catch (Args.ParseException e) {
            if (e.getMessage() != null)
                System.out.println(e.getMessage() + "\n");

            System.out.println(usage);

            System.exit(1);
        }

        String jsonCfgStr = null;

        if (parsedArgs.config != null)
            jsonCfgStr = Files.readString(parsedArgs.config);

        var ignition = new IgnitionImpl();

        ignition.start(parsedArgs.nodeName, jsonCfgStr);
    }

    /**
     * Simple value object with parsed CLI args of ignite runner.
     */
    private static class Args {
        /** Name of the node. */
        private final String nodeName;

        /** Path to config file. */
        private final Path config;

        /**
         * Creates new instance with parsed arguments.
         *
         * @param nodeName Name of the node.
         * @param config Path to config file.
         */
        private Args(String nodeName, Path config) {
            this.nodeName = nodeName;
            this.config = config;
        }

        /**
         * Simple CLI arguments parser.
         *
         * @param args CLI arguments.
         * @return Parsed arguments.
         * @throws ParseException if required args are absent.
         */
        private static Args parseArgs(String[] args) throws ParseException {
            if (args.length == 1)
                return new Args(args[0], null);
            else if (args.length == 3) {
                if ("--config".equals(args[0])) {
                    try {
                        return new Args(args[2], Path.of(args[1]));
                    }
                    catch (InvalidPathException e) {
                        throw new ParseException("Couldn't parse configuration path.");
                    }
                }
                else
                    throw new ParseException();
            }
            else
                throw new ParseException();
        }

        /**
         * Exception for indicate any problems with parsing CLI args.
         */
        private static class ParseException extends IgniteInternalCheckedException {
            /**
             * Creates new exception of parsing.
             *
             * @param msg Message.
             */
            private ParseException(String msg) {
                super(msg);
            }

            /**
             * Creates new exception of parsing.
             */
            private ParseException() {
            }
        }
    }
}
