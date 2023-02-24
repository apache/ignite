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

package org.apache.ignite.internal.commandline.walreader;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgumentParser;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteExperimental;
import org.jetbrains.annotations.Nullable;
import static java.util.Arrays.asList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;
import static org.apache.ignite.internal.IgniteKernal.NL;
import static org.apache.ignite.internal.IgniteVersionUtils.ACK_VER_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.internal.commandline.CommandHandler.setupJavaLogger;
import static org.apache.ignite.internal.commandline.argument.parser.CLIArgument.mandatoryArg;
import static org.apache.ignite.internal.commandline.argument.parser.CLIArgument.optionalArg;
import static org.apache.ignite.startup.cmdline.CommandLineStartup.isHelp;

/**
 * This class defines command-line WAL reader startup. This startup can be used to start Ignite
 * WAL reader application outside any hosting environment from command line.
 * This startup is a Java application with {@link #main(String[])} method that accepts command line arguments.
 */
@IgniteExperimental
public class WalReaderCommandLineStartup {
    /** Quite log flag. */
    private static final boolean QUITE = IgniteSystemProperties.getBoolean(IGNITE_QUIET);

    /** Binary metadata directory argument. */
    public static final String BINARY_METADATA_DIR_ARG = "--binary-metadata";

    /** Marshaller mapping directory argument. */
    public static final String MARSHALLER_MAPPING_DIR_ARG = "--marshaller-mapping";

    /** Marshaller mapping directory argument. */
    public static final String WAL_DIR_ARG = "--wal";

    /** Types to log argument. */
    public static final String TYPES_ARG = "--types";

    /** Page size argument. */
    public static final String PAGE_SIZE_ARG = "--page-size";

    /** Print pointer argument. */
    public static final String PRINT_POINTER_ARG = "--print-pointer";

    /** Print pointer argument. */
    public static final String PRINT_ONLY_TYPES_ARG = "--print-only-types";

    /** Print pointer. */
    private boolean printPointer;

    /** Print pointer. */
    private boolean printOnlyTypes;

    /**
     * Main entry point.
     *
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        if (!QUITE) {
            X.println("   __________  ________________  _      _____   __     ___  _______   ___  _______ " + NL +
                "  /  _/ ___/ |/ /  _/_  __/ __/ | | /| / / _ | / /    / _ \\/ __/ _ | / _ \\/ __/ _ \\" + NL +
                " _/ // (_ /    // /  / / / _/   | |/ |/ / __ |/ /__  / , _/ _// __ |/ // / _// , _/" + NL +
                "/___/\\___/_/|_/___/ /_/ /___/   |__/|__/_/ |_/____/ /_/|_/___/_/ |_/____/___/_/|_| " + NL +
                "                                                                                   ");
            X.println("Ignite WAL Reader Command Line Startup, ver. " + ACK_VER_STR);
            X.println(COPYRIGHT);
            X.println();
        }

        if (args.length > 0 && isHelp(args[0]))
            exit(null, true, 0);

        new WalReaderCommandLineStartup().readWal(Arrays.asList(args));

        exit(null, false, EXIT_CODE_OK);
    }

    /** Executes WAL read. */
    private void readWal(List<String> args) {
        try {
            IgniteLogger log = setupJavaLogger("wal-reader", WalReaderCommandLineStartup.class);

            try (WALIterator iter = iteratorFromArgs(args)) {
                while (iter.hasNext()) {
                    IgniteBiTuple<WALPointer, WALRecord> rec = iter.next();

                    if (printOnlyTypes)
                        log.info(rec.get2().type().name() + (printPointer ? (" " + rec.get1()) : ""));
                    else {
                        if (printPointer)
                            log.info(String.valueOf(rec.get1()));

                        log.info(String.valueOf(rec.get2()));
                    }
                }
            }
        }
        catch (IllegalArgumentException e) {
            exit("Invalid arguments: " + e.getMessage(), false, EXIT_CODE_INVALID_ARGUMENTS);
        }
        catch (Throwable e) {
            e.printStackTrace();

            String note = "";

            if (X.hasCause(e, ClassNotFoundException.class))
                note = "\nNote! You may use 'USER_LIBS' environment variable to specify your classpath.";

            exit("Failed to run WAL reader: " + e.getMessage() + note, false, EXIT_CODE_UNEXPECTED_ERROR);
        }
    }

    /** @return WAL iterator. */
    private WALIterator iteratorFromArgs(
        List<String> args
    ) throws IgniteCheckedException {
        CLIArgumentParser p = argumentsParser();

        try {
            p.parse(args.iterator());
        }
        catch (IgniteException e) {
            exit("Invalid arguments: " + e.getMessage(), false, EXIT_CODE_INVALID_ARGUMENTS);
        }

        IteratorParametersBuilder builder = IteratorParametersBuilder.withIteratorParameters()
            .marshallerMappingFileStoreDir(new File(p.<String>get(MARSHALLER_MAPPING_DIR_ARG)))
            .binaryMetadataFileStoreDir(new File(p.<String>get(BINARY_METADATA_DIR_ARG)))
            .filesOrDirs(new File(p.<String>get(WAL_DIR_ARG)))
            .pageSize(p.get(PAGE_SIZE_ARG));

        final Set<WALRecord.RecordType> types = p.get(TYPES_ARG) == null
            ? null
            : Arrays.stream(p.<String[]>get(TYPES_ARG))
                .map(WALRecord.RecordType::valueOf)
                .collect(Collectors.toSet());

        builder.filter((type, ptr) -> types == null || types.contains(type));

        builder.validate();

        printPointer = p.get(PRINT_POINTER_ARG);
        printOnlyTypes = p.get(PRINT_ONLY_TYPES_ARG);

        return new IgniteWalIteratorFactory().iterator(builder);
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

        if (showUsage)
            X.error(argumentsParser().usage());

        System.exit(exitCode);
    }

    /** @return Arguments parser */
    private static CLIArgumentParser argumentsParser() {
        return new CLIArgumentParser(asList(
            mandatoryArg(
                MARSHALLER_MAPPING_DIR_ARG,
                "Path to the marshaller mapping",
                String.class
            ),
            mandatoryArg(
                BINARY_METADATA_DIR_ARG,
                "Path to the binary metadata",
                String.class
            ),
            mandatoryArg(
                WAL_DIR_ARG,
                "File or directory with WAL segments",
                String.class
            ),
            optionalArg(PAGE_SIZE_ARG, "Page size", Integer.class, () -> DFLT_PAGE_SIZE),
            optionalArg(PRINT_POINTER_ARG, "Print WAL pointer flag", Boolean.class),
            optionalArg(PRINT_ONLY_TYPES_ARG, "Print only record types flag", Boolean.class),
            optionalArg(TYPES_ARG, "WAL records types to print", String[].class)
        ));
    }
}
