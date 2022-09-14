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

package org.apache.ignite.internal.performancestatistics;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.performancestatistics.handlers.PrintHandler;
import org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsReader;
import org.apache.ignite.internal.processors.performancestatistics.OperationType;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;

/**
 * Performance statistics printer.
 */
public class PerformanceStatisticsPrinter {
    /**
     * @param args Program arguments or '-h' to get usage help.
     */
    public static void main(String... args) throws Exception {
        Parameters params = parseArguments(args);

        validateParameters(params);

        PrintStream ps = printStream(params.outFile);

        try {
            new FilePerformanceStatisticsReader(
                new PrintHandler(ps, params.ops, params.from, params.to, params.cacheIds))
                .read(singletonList(new File(params.statFileOrDir)));
        }
        finally {
            if (params.outFile != null)
                ps.close();
        }
    }

    /**
     * Parses arguments or print help.
     *
     * @param args Arguments to parse.
     * @return Program arguments.
     */
    private static Parameters parseArguments(String[] args) {
        if (args == null || args.length == 0 || "--help".equalsIgnoreCase(args[0]) || "-h".equalsIgnoreCase(args[0]))
            printHelp();

        Parameters params = new Parameters();

        Iterator<String> iter = Arrays.asList(args).iterator();

        params.statFileOrDir = iter.next();

        while (iter.hasNext()) {
            String arg = iter.next();

            if ("--out".equalsIgnoreCase(arg)) {
                A.ensure(iter.hasNext(), "Expected output file name");

                params.outFile = iter.next();
            }
            else if ("--ops".equalsIgnoreCase(arg)) {
                A.ensure(iter.hasNext(), "Expected operation types");

                String[] ops = iter.next().split(",");

                A.ensure(ops.length > 0, "Expected at least one operation");

                params.ops = new BitSet();

                for (String op : ops) {
                    OperationType opType = enumIgnoreCase(op, OperationType.class);

                    A.ensure(opType != null, "Unknown operation type [op=" + op + ']');

                    params.ops.set(opType.id());
                }
            }
            else if ("--from".equalsIgnoreCase(arg)) {
                A.ensure(iter.hasNext(), "Expected time from");

                params.from = Long.parseLong(iter.next());
            }
            else if ("--to".equalsIgnoreCase(arg)) {
                A.ensure(iter.hasNext(), "Expected time to");

                params.to = Long.parseLong(iter.next());
            }
            else if ("--caches".equalsIgnoreCase(arg)) {
                A.ensure(iter.hasNext(), "Expected cache names");

                String[] caches = iter.next().split(",");

                A.ensure(caches.length > 0, "Expected at least one cache name");

                params.cacheIds = Arrays.stream(caches).map(CU::cacheId).collect(Collectors.toSet());
            }
            else
                throw new IllegalArgumentException("Unknown command: " + arg);
        }

        return params;
    }

    /** Prints help. */
    private static void printHelp() {
        String ops = Arrays.stream(OperationType.values()).map(Enum::toString).collect(joining(", "));

        System.out.println("The script is used to print performance statistics files to the console or file." +
            U.nl() + U.nl() +
            "Usage: print-statistics.sh path_to_files [--out out_file] [--ops op_types] " +
            "[--from startTimeFrom] [--to startTimeTo] [--caches cache_names]" + U.nl() +
            U.nl() +
            "  path_to_files - Performance statistics file or files directory." + U.nl() +
            "  out_file      - Output file." + U.nl() +
            "  op_types      - Comma separated list of operation types to filter the output." + U.nl() +
            "  from          - The minimum operation start time to filter the output." + U.nl() +
            "  to            - The maximum operation start time to filter the output." + U.nl() +
            "  cache_names   - Comma separated list of cache names to filter the output." + U.nl() +
            U.nl() +
            "Times must be specified in the Unix time format in milliseconds." + U.nl() +
            "List of operation types: " + ops + '.');

        System.exit(0);
    }

    /** @param params Validates parameters. */
    private static void validateParameters(Parameters params) {
        File statFileOrDir = new File(params.statFileOrDir);

        A.ensure(statFileOrDir.exists(), "Performance statistics file or files directory does not exists");
    }

    /** @return Print stream to the console or file. */
    private static PrintStream printStream(@Nullable String outFile) {
        PrintStream ps;

        if (outFile != null) {
            try {
                ps = new PrintStream(new BufferedOutputStream(new FileOutputStream(new File(outFile), true)));
            }
            catch (IOException e) {
                throw new IllegalArgumentException("Cannot write to output file", e);
            }
        }
        else
            ps = System.out;

        return ps;
    }

    /**
     * Gets the enum for the given name ignore case.
     *
     * @param name Enum name.
     * @param cls Enum class.
     * @return The enum or {@code null} if not found.
     */
    private static <E extends Enum<E>> @Nullable E enumIgnoreCase(String name, Class<E> cls) {
        for (E e : cls.getEnumConstants()) {
            if (e.name().equalsIgnoreCase(name))
                return e;
        }

        return null;
    }

    /** Printer parameters. */
    private static class Parameters {
        /** Performance statistics file or files directory. */
        private String statFileOrDir;

        /** Output file. */
        @Nullable private String outFile;

        /** Operation types to print. */
        @Nullable private BitSet ops;

        /** The minimum operation start time to filter the output. */
        private long from = Long.MIN_VALUE;

        /** The maximum operation start time to filter the output. */
        private long to = Long.MAX_VALUE;

        /** Cache identifiers to filter the output. */
        @Nullable private Set<Integer> cacheIds;
    }
}
