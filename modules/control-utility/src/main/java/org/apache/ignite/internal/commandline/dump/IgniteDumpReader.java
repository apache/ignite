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

package org.apache.ignite.internal.commandline.dump;

import java.io.File;
import java.net.URL;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.dump.Dump;
import org.apache.ignite.dump.DumpConsumer;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgumentParser;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.DumpImpl;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.ConsoleLogger;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.spring.IgniteSpringHelper;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

import static java.util.Arrays.asList;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_BINARY_METADATA_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_MARSHALLER_PATH;
import static org.apache.ignite.internal.IgniteComponentType.SPRING;
import static org.apache.ignite.internal.commandline.argument.parser.CLIArgument.mandatoryArg;
import static org.apache.ignite.internal.commandline.argument.parser.CLIArgument.optionalArg;

/**
 * Dump Reader application.
 * The application runs independently of Ignite node process and provides the ability to the {@link DumpConsumer} to consume
 * all data stored in cache dump ({@link Dump})
 */
public class IgniteDumpReader implements Runnable {
    /** */
    public static final String DUMP = "--dump";

    /** */
    public static final String PRINT = "--print";

    /** */
    public static final String FORMAT = "--format";

    /** */
    public static final String RESTORE = "--restore";

    /** */
    public static final String THIN = "--thin";

    /** */
    public static final String THIN_CONFIG = "--thin-config";

    /** */
    public static final String CLIENT_NODE_CONFIG = "--client-node-config";

    /** */
    public static final String USE_DATA_STREAMER = "--use-data-streamer";

    /** */
    public static final String CONSUMER = "--consumer";

    /** Configuration. */
    private final DumpReaderConfiguration cfg;

    /**
     * @param cfg Dump reader configuration.
     */
    public IgniteDumpReader(DumpReaderConfiguration cfg) {
        this.cfg = cfg;
    }

    /**
     * Entry point.
     * @param args Arguments.
     */
    public static void main(String[] args) throws Exception {
        new IgniteDumpReader(configFromArgs(args)).run();
    }

    /** {@inheritDoc} */
    @Override public void run() {
        try {
            GridKernalContext kctx = startStandaloneKernal(cfg.dir());

            Dump dump = new DumpImpl(kctx, cfg.dir());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @return Kernal instance.
     * @throws IgniteCheckedException If failed.
     */
    private StandaloneGridKernalContext startStandaloneKernal(File dumpDir) throws IgniteCheckedException {
        File binaryMeta = new File(dumpDir, DFLT_BINARY_METADATA_PATH);
        File marshaller = new File(dumpDir, DFLT_MARSHALLER_PATH);

        A.ensure(binaryMeta.exists() && binaryMeta.isDirectory(), "Binary meta directory must exists: " + binaryMeta.getAbsolutePath());
        A.ensure(marshaller.exists() && marshaller.isDirectory(), "Marshaller directory must exists: " + marshaller.getAbsolutePath());

        StandaloneGridKernalContext kctx = new StandaloneGridKernalContext(ConsoleLogger.INSTANCE, binaryMeta, marshaller);

        for (GridComponent comp : kctx)
            comp.start();

        return kctx;
    }

    /**
     * @param args CMD arguments.
     * @return Configuration.
     */
    private static DumpReaderConfiguration configFromArgs(String[] args) throws Exception {
        CLIArgumentParser p = new CLIArgumentParser(asList(
            mandatoryArg(DUMP, "Dump directory", String.class),
            optionalArg(PRINT, "Print dump content to standard output. Format can be set by --format. Default is json", boolean.class),
            optionalArg(FORMAT, "Format of output cache dump data", PrintDumpConsumer.OutputFormat.class),
            optionalArg(RESTORE, "Restores all data stored in dump to Ignite cluster.", boolean.class),
            optionalArg(THIN, "Addresses for thin client to connect to", String[].class),
            optionalArg(THIN_CONFIG, "Path to spring xml file containing definition of ClientConfiguration", String.class),
            optionalArg(
                CLIENT_NODE_CONFIG,
                "Path to spring xml file containing definition of Ignite client node configuration",
                String.class
            ),
            optionalArg(USE_DATA_STREAMER, "Enables usage of data streamer to load dump data to cluster", boolean.class),
            optionalArg(CONSUMER, "Class name of custom DumpConsumer. See javadoc for more details", String.class)
        ));

        if (args.length == 0) {
            System.out.println(p.usage());

            System.exit(0);
        }

        p.parse(asList(args).iterator());

        File dir = new File(p.<String>get(DUMP));

        A.ensure(dir.exists() && dir.isDirectory(), "Dump directory not exists");

        check(p);

        DumpConsumer cnsmr = null;

        if (p.get(PRINT))
            throw new IllegalArgumentException(PRINT + " not supported for now!");
        else if (p.get(CONSUMER) != null)
            throw new IllegalArgumentException(CONSUMER + " not supported for now!");
        else if (p.get(RESTORE)) {
            if (p.get(CLIENT_NODE_CONFIG) != null)
                throw new IllegalArgumentException(CLIENT_NODE_CONFIG + " not supported for now!");
            else {
                ClientConfiguration cliCfg;

                if (p.get(THIN) != null)
                    cliCfg = new ClientConfiguration().setAddresses(p.get(THIN));
                else {
                    URL cfgUrl = U.resolveSpringUrl(p.get(THIN_CONFIG));

                    IgniteSpringHelper spring = SPRING.create(false);

                    IgniteBiTuple<Map<Class<?>, Collection>, ? extends GridSpringResourceContext> cfgs =
                        spring.loadBeans(cfgUrl, ClientConfiguration.class);

                    Collection<ClientConfiguration> cliCfgs = cfgs.get1().get(ClientConfiguration.class);

                    if (F.size(cliCfgs) != 1) {
                        throw new IgniteCheckedException(
                            "Exact 1 CaptureDataChangeConfiguration configuration should be defined. Found " + F.size(cliCfgs)
                        );
                    }

                    cliCfg = cliCfgs.iterator().next();
                }

                cnsmr = new RestoreThinClientDumpConsumer(cliCfg);
            }

        }

        return new DumpReaderConfiguration(dir, cnsmr);
    }

    /** */
    private static void check(CLIArgumentParser p) {
        A.ensure(
            (p.get(PRINT) ? 1 : 0) + (p.get(RESTORE) ? 1 : 0) + (p.get(CONSUMER) != null ? 1 : 0) == 1,
            "One of " + PRINT + ", " + RESTORE + ", " + CONSUMER + " must be specified"
        );

        if (p.get(RESTORE)) {
            A.ensure(
                (p.get(THIN) != null ? 1 : 0) + (p.get(THIN_CONFIG) != null ? 1 : 0) + (p.get(CLIENT_NODE_CONFIG) != null ? 1 : 0) == 1,
                "One of " + PRINT + ", " + RESTORE + ", " + CONSUMER + " must be specified"
            );

            A.ensure(
                p.<Boolean>get(USE_DATA_STREAMER) == null || p.get(CLIENT_NODE_CONFIG) != null,
                USE_DATA_STREAMER + " can be used only with client node"
            );
        }
    }

    /** */
    private static class DumpReaderConfiguration {
        /** */
        final File dir;

        /** */
        final DumpConsumer cnsmr;

        /** */
        public DumpReaderConfiguration(File dir, DumpConsumer cnsmr) {
            this.dir = dir;
            this.cnsmr = cnsmr;
        }

        /** */
        public File dir() {
            return dir;
        }

        /** */
        public DumpConsumer consumer() {
            return cnsmr;
        }
    }
}
