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

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_BINARY_METADATA_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_MARSHALLER_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH;
import static org.apache.ignite.configuration.PersistentStoreConfiguration.DFLT_WAL_STORE_PATH;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_NAME_PATTERN;

/**
 * CDC(Capture Data Change) application.
 */
public class IgniteCDC implements Runnable {
    /** Ignite configuration. */
    private final IgniteConfiguration cfg;

    /** Events consumers. */
    private final CDCConsumer consumer;

    /** Logger. */
    private final IgniteLogger log;

    /** WAL iterator factory. */
    private final IgniteWalIteratorFactory factory;

    /** Watch utils. */
    private final WatchUtils wu;

    /** Work dir. */
    private Path workDir;

    /** Consistent id directory name. */
    private String consIdDir;

    /** Binary metadata directory. */
    private File binaryMeta;

    /** Marshaller directory. */
    private File marshaller;

    /** WAL files queue. */
    private final BlockingQueue<Path> walFiles = new LinkedBlockingDeque<>();

    /**
     * @param cfg Ignite configuration.
     * @param consumer Event consumer.
     */
    public IgniteCDC(IgniteConfiguration cfg, CDCConsumer consumer) {
        this.cfg = cfg;
        this.consumer = consumer;
        this.log = cfg.getGridLogger().getLogger(IgniteCDC.class);
        this.factory = new IgniteWalIteratorFactory(log);
        this.wu = new WatchUtils(log);

        if (cfg.getConsistentId() != null)
            consIdDir = U.maskForFileName(cfg.getConsistentId().toString());
    }

    /** {@inheritDoc} */
    @Override public void run() {
        if (log.isInfoEnabled()) {
            log.info("Starting Ignite CDC Application.");
            log.info(consumer.toString());
            log.info("--------------------------------");
        }

        consumer.start(cfg);

        final Path[] nodeWalDir = new Path[1];

        findNodeWalDir(dir -> nodeWalDir[0] = dir);

        if (log.isInfoEnabled())
            log.info("Node WAL directory found[dir=" + nodeWalDir[0] + ']');

        Thread newFileThread = new Thread(() ->
            wu.waitFor(nodeWalDir[0], p -> WAL_NAME_PATTERN.matcher(p.getFileName().toString()).matches(), walFile -> {
                try {
                    walFiles.put(walFile);

                    return true;
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            })
        );

        newFileThread.start();

        try {
            Path walFile = null;
            Optional<WALPointer> pos = Optional.empty();

            while (true) {
                Path newWalFile = walFiles.poll(1, TimeUnit.SECONDS);

                if (newWalFile != null) {
                    log.info("Found new WAL file[file=" + newWalFile + ']');

                    if (walFile != null)
                        readAvailableEvents(walFile, pos);

                    pos = Optional.empty();
                    walFile = newWalFile;
                }

                if (walFile == null)
                    continue;

                pos = readAvailableEvents(walFile, pos);
            }
        }
        catch (InterruptedException | IgniteCheckedException e) {
            log.info("IgniteCDC stopped.");
        }
    }

    /**
     * @param walFile WAL file.
     * @param pos Last readed position.
     * @return Last readed position.
     * @throws IgniteCheckedException In case of error.
     */
    private Optional<WALPointer> readAvailableEvents(Path walFile, Optional<WALPointer> pos)
        throws IgniteCheckedException {
        if (log.isInfoEnabled())
            log.info("Fetching new events from walFile[file=" + walFile + ']');

        IteratorParametersBuilder builder = new IteratorParametersBuilder()
            .log(log)
            .binaryMetadataFileStoreDir(binaryMeta)
            .marshallerMappingFileStoreDir(marshaller)
            .keepBinary(true)
            .filesOrDirs(walFile.toFile());

        pos.ifPresent(ptr -> builder.from((FileWALPointer)ptr));

        try (WALIterator it = factory.iterator(builder)) {
            while (it.hasNext()) {
                WALRecord rec = it.next().get2();

                consumer.onRecord(rec);
            }

            return it.lastRead();
        }
    }

    /**
     * Finds node wal directory.
     *
     * @param callback Callback to be notified.
     */
    private void findNodeWalDir(Consumer<Path> callback) {
        wu.waitFor(new File(cfg.getWorkDirectory()).toPath(), work -> {
            workDir = work;

            if (log.isDebugEnabled())
                log.debug("Work directory found[dir=" + work + ']');

            wu.waitFor(Paths.get(work.toAbsolutePath().toString(), DFLT_STORE_DIR), db -> {
                if (log.isDebugEnabled())
                    log.debug("DB directory found[dir=" + work + ']');

                wu.waitFor(Paths.get(work.toAbsolutePath().toString(), DFLT_WAL_STORE_PATH), wal -> {
                    if (log.isDebugEnabled())
                        log.debug("WAL directory found[dir=" + wal + ']');

                    Consumer<Path> preCallback = p -> resolveDirs();

                    if (consIdDir != null)
                        wu.waitFor(Paths.get(wal.toAbsolutePath().toString(), consIdDir),
                            preCallback.andThen(callback));
                    else {
                        log.info("Consistent id not specified. Waiting for first WAL directory available[parent=" +
                            wal + ']');

                        wu.waitFor(wal, p -> !p.endsWith(DFLT_WAL_ARCHIVE_PATH), p -> {
                            consIdDir = p.subpath(p.getNameCount() - 1, p.getNameCount()).toString();

                            if (log.isInfoEnabled())
                                log.info("Found consistenId directory[consId=" + consIdDir + ']');

                            preCallback.andThen(callback);
                        });
                    }
                });
            });
        });
    }

    /** Resolves directories required for WAL iteration. */
    private void resolveDirs() {
        try {
            binaryMeta =
                new File(U.resolveWorkDirectory(workDir.toString(), DFLT_BINARY_METADATA_PATH, false), consIdDir);

            marshaller = U.resolveWorkDirectory(workDir.toString(), DFLT_MARSHALLER_PATH, false);

            if (log.isInfoEnabled()) {
                log.info("Using BinaryMeta directory[dir=" + binaryMeta + ']');
                log.info("Using Marshaller directory[dir=" + marshaller + ']');
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }
}
