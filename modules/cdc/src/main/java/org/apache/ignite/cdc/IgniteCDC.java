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

package org.apache.ignite.cdc;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_BINARY_METADATA_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_CDC_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_MARSHALLER_PATH;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor.NODE_PATTERN;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor.UUID_STR_PATTERN;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_NAME_PATTERN;

/**
 * CDC(Capture Data Change) application.
 */
public class IgniteCDC implements Runnable, Closeable {
    /** Ignite configuration. */
    private final IgniteConfiguration cfg;

    /** WAL iterator factory. */
    private final IgniteWalIteratorFactory factory;

    /** Events consumers. */
    private final CDCConsumer consumer;

    /** Logger. */
    private final IgniteLogger log;

    /** Watch utils. */
    private final WatchUtils wu;

    /** CDC directory. */
    private Path cdcDir;

    /** Binary meta directory. */
    private File binaryMeta;

    /** Marshaller directory. */
    private File marshaller;

    /** Thread that waits for creation of the new WAL segments. */
    private Thread waitSegmentThread;

    /** Queue of segments to process. */
    private final BlockingQueue<Path> segments = new LinkedBlockingQueue<>();

    /**
     * @param cfg Ignite configuration.
     * @param consumer Event consumer.
     */
    public IgniteCDC(IgniteConfiguration cfg, CDCConsumer consumer) {
        this.log = logger(cfg, workDir(cfg));
        this.cfg = cfg;
        this.factory = new IgniteWalIteratorFactory(log);
        this.consumer = consumer;
        this.wu = new WatchUtils(log);

        if (!CU.isPersistenceEnabled(cfg))
            throw new IllegalArgumentException("Persistence disabled. IgniteCDC can't run!");

        if (cfg.getConsistentId() == null)
            log.warning("Consistent ID is not set, it is recommended to set consistent ID for production " +
                "clusters (use IgniteConfiguration.setConsistentId property)");
    }

    /** {@inheritDoc} */
    @Override public void run() {
        if (log.isInfoEnabled()) {
            log.info("Starting Ignite CDC Application.");
            log.info("Consumer    -\t" + consumer.toString());
        }

        try {
            initDirs();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        if (log.isInfoEnabled()) {
            log.info("CDC dir     -\t" + cdcDir);
            log.info("Binary meta -\t" + binaryMeta);
            log.info("Marshaller  -\t" + marshaller);
            log.info("--------------------------------");
        }

        consumer.start(cfg, log);

        waitSegmentThread = new Thread(() -> wu.waitFor(cdcDir,
            p -> WAL_NAME_PATTERN.matcher(p.getFileName().toString()).matches(),
            segment -> {
                log.info("Found new segment - " + segment);

                try {
                    segments.put(segment);
                }
                catch (InterruptedException e) {
                    throw new IgniteException(e);
                }

                return true;
            }),
            "wait-archive-segments");

        waitSegmentThread.start();

        while (true) {
            try {
                Path segment = segments.take();

                readSegment(segment.toFile());

                Files.delete(segment);
            }
            catch (IgniteCheckedException | InterruptedException | IOException e) {
                throw new IgniteException(e);
            }
        }
    }

    /** Reads all available from segment. */
    private void readSegment(File segment) throws IgniteCheckedException {
        IgniteWalIteratorFactory.IteratorParametersBuilder builder = new IgniteWalIteratorFactory.IteratorParametersBuilder()
            .log(log)
            .binaryMetadataFileStoreDir(binaryMeta)
            .marshallerMappingFileStoreDir(marshaller)
            .keepBinary(true)
            .filesOrDirs(segment);

        try (WALIterator it = factory.iterator(builder)) {
            while (it.hasNext())
                consumer.onRecord(it.next().get2());
        }
    }

    /** Founds required directories. */
    private void initDirs() throws IgniteCheckedException {
        String workDir = workDir(cfg);

        cdcDir = initCdcDir(workDir);

        String consIdDir = cdcDir.getName(cdcDir.getNameCount() - 1).toString();

        if (log.isDebugEnabled())
            log.debug("Found WAL archive[dir=" + cdcDir + ']');

        binaryMeta = new File(U.resolveWorkDirectory(workDir, DFLT_BINARY_METADATA_PATH, false), consIdDir);

        marshaller = U.resolveWorkDirectory(workDir, DFLT_MARSHALLER_PATH, false);

        if (log.isDebugEnabled()) {
            log.debug("Using BinaryMeta directory[dir=" + binaryMeta + ']');
            log.debug("Using Marshaller directory[dir=" + marshaller + ']');
        }
    }

    /**
     * @param workDir Working directory.
     * @return WAL archive directory.
     */
    private Path initCdcDir(String workDir) {
        Path cdcParent;

        if (cfg.getDataStorageConfiguration() != null &&
            !F.isEmpty(cfg.getDataStorageConfiguration().getCdcPath())) {
            cdcParent = Paths.get(cfg.getDataStorageConfiguration().getCdcPath());

            if (!cdcParent.isAbsolute())
                cdcParent = Paths.get(workDir, cfg.getDataStorageConfiguration().getCdcPath());
        }
        else
            cdcParent = Paths.get(workDir).resolve(DFLT_CDC_PATH);

        if (log.isDebugEnabled())
            log.debug("Archive root[dir=" + cdcParent + ']');

        final Path[] cdcDir = new Path[1];

        wu.waitFor(cdcParent,
            dir -> dir.getName(dir.getNameCount() - 1).toString().matches(NODE_PATTERN + UUID_STR_PATTERN),
            dir -> { cdcDir[0] = dir; });

        return cdcDir[0];
    }


    /**
     * Initialize logger.
     *
     * @param cfg Configuration.
     */
    private static IgniteLogger logger(IgniteConfiguration cfg, String workDir) {
        try {
            UUID appLogId = UUID.randomUUID();

            return IgnitionEx.IgniteNamedInstance.initLogger(cfg.getGridLogger(), appLogId, workDir);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Resolves work directory.
     *
     * @return Working directory
     */
    public static String workDir(IgniteConfiguration cfg) {
        try {
            String igniteHome = cfg.getIgniteHome();

            // Set Ignite home.
            if (igniteHome == null)
                igniteHome = U.getIgniteHome();
            else
                // If user provided IGNITE_HOME - set it as a system property.
                U.setIgniteHome(igniteHome);

            String userProvidedWorkDir = cfg.getWorkDirectory();

            // Correctly resolve work directory and set it back to configuration.
            return U.workDirectory(userProvidedWorkDir, igniteHome);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        waitSegmentThread.interrupt();
    }
}
