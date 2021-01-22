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

package org.apache.ignite.internal.cdc;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_BINARY_METADATA_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_CDC_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_MARSHALLER_PATH;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor.NODE_PATTERN;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor.UUID_STR_PATTERN;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_NAME_PATTERN;

/**
 * CDC(Capture Data Change) application.
 */
public class IgniteCDC implements Runnable {
    /** System property to specify consistent id for CDC. */
    public static final String IGNITE_CDC_CONSISTENT_ID = "IGNITE_CDC_CONSISTENT_ID";

    /** System property to specify node index for CDC. */
    public static final String IGNITE_CDC_NODE_IDX = "IGNITE_CDC_NODE_IDX";

    /** System property to specify lock file timeout for CDC. */
    public static final String IGNITE_CDC_LOCK_TIMEOUT = "IGNITE_CDC_LOCK_TIMEOUT";

    /** State dir. */
    public static final String STATE_DIR = "state";

    /** Default lock timeout. */
    public static final int DFLT_LOCK_TIMEOUT = 1000;

    /** Ignite configuration. */
    private final IgniteConfiguration cfg;

    /** WAL iterator factory. */
    private final IgniteWalIteratorFactory factory;

    /** Events consumers. */
    private final CDCConsumer consumer;

    /** Keep binary flag. */
    private boolean keepBinary;

    /** Logger. */
    private final IgniteLogger log;

    /** CDC directory. */
    private Path cdcDir;

    /** Binary meta directory. */
    private File binaryMeta;

    /** Marshaller directory. */
    private File marshaller;

    /** CDC state. */
    private CDCConsumerState state;

    /** Save state to start from. */
    private WALPointer initState;

    /** Consistent ID. */
    private final String nodeDir;

    /** Previous segments. */
    private final List<Path> prevSegments = new ArrayList<>();

    /**
     * @param cfg Ignite configuration.
     * @param consumer Event consumer.
     */
    public IgniteCDC(IgniteConfiguration cfg, CDCConsumer consumer) {
        this.cfg = cfg;
        this.consumer = consumer;

        log = logger(cfg, workDir(cfg));
        factory = new IgniteWalIteratorFactory(log);

        if (!CU.isPersistenceEnabled(cfg))
            throw new IllegalArgumentException("Persistence disabled. IgniteCDC can't run!");

        nodeDir = consistentId(cfg);

        if (nodeDir == null) {
            log.warning("Can't determine nodeDir. It is recommended to set Consistent ID for production " +
                "clusters (use IgniteConfiguration.setConsistentId or " + IGNITE_CDC_CONSISTENT_ID + ", " +
                IGNITE_CDC_NODE_IDX + " property)");
        }
    }

    /** Runs CDC. */
    @Override public void run() {
        try {
            runX();
        }
        catch (Exception e) {
            e.printStackTrace();

            throw new RuntimeException(e);
        }
    }

    /** Runs CDC application with possible exception. */
    public void runX() throws Exception {
        if (log.isInfoEnabled()) {
            log.info("Starting Ignite CDC Application.");
            log.info("Consumer    -\t" + consumer.toString());
        }

        cdcDir = findCDCDir(workDir(cfg));

        try (CDCFileLockHolder lock =
                new CDCFileLockHolder(cdcDir.toString(), consumer::id, log)) {
            log.info("Trying to acquire file lock[lock=" + lock.lockPath() + ']');

            lock.tryLock(IgniteSystemProperties.getInteger(IGNITE_CDC_LOCK_TIMEOUT, DFLT_LOCK_TIMEOUT));

            init();

            if (log.isInfoEnabled()) {
                log.info("CDC dir     -\t" + cdcDir);
                log.info("Binary meta -\t" + binaryMeta);
                log.info("Marshaller  -\t" + marshaller);
                log.info("--------------------------------");
            }

            state = new CDCConsumerState(cdcDir.resolve(STATE_DIR), consumer.id());

            initState = state.load();

            if (initState != null && log.isInfoEnabled())
                log.info("Loaded initial state[state=" + initState + ']');

            consumer.start(cfg, log);

            keepBinary = consumer.keepBinary();

            try {
                Predicate<Path> walFilesOnly = p -> WAL_NAME_PATTERN.matcher(p.getFileName().toString()).matches();

                Comparator<Path> sortByNumber = Comparator.comparingLong(this::segmentNumber);

                waitFor(cdcDir, walFilesOnly, sortByNumber, segment -> {
                    try {
                        readSegment(segment);

                        return true;
                    }
                    catch (IgniteCheckedException | IOException e) {
                        throw new IgniteException(e);
                    }
                }, log);
            }
            finally {
                consumer.stop();

                if (log.isInfoEnabled())
                    log.info("Ignite CDC Application stoped.");
            }
        }
    }

    /** Founds required directories. */
    private void init() throws IgniteCheckedException, IOException {
        String workDir = workDir(cfg);
        String consIdDir = cdcDir.getName(cdcDir.getNameCount() - 1).toString();

        Files.createDirectories(cdcDir.resolve(STATE_DIR));

        binaryMeta = new File(U.resolveWorkDirectory(workDir, DFLT_BINARY_METADATA_PATH, false), consIdDir);

        marshaller = U.resolveWorkDirectory(workDir, DFLT_MARSHALLER_PATH, false);

        if (log.isDebugEnabled()) {
            log.debug("Using BinaryMeta directory[dir=" + binaryMeta + ']');
            log.debug("Using Marshaller directory[dir=" + marshaller + ']');
        }
    }

    /** Reads all available from segment. */
    private void readSegment(Path segment) throws IgniteCheckedException, IOException {
        log.info("Processing WAL segment[segment=" + segment + ']');

        IgniteWalIteratorFactory.IteratorParametersBuilder builder = new IgniteWalIteratorFactory.IteratorParametersBuilder()
            .log(log)
            .binaryMetadataFileStoreDir(binaryMeta)
            .marshallerMappingFileStoreDir(marshaller)
            .keepBinary(keepBinary)
            .filesOrDirs(segment.toFile());

        if (initState != null) {
            long segmentIdx = segmentNumber(segment);

            if (segmentIdx > initState.index()) {
                log.error("Found segment greater then saved state. Some events are missed. Exiting!" +
                    "[state=" + initState + ",segment=" + segmentIdx + ']');

                throw new IgniteException("Some data missed.");
            }
            else if (segmentIdx < initState.index()) {
                if (log.isInfoEnabled()) {
                    log.info("Deleting segment. Saved state has greater index.[segment=" +
                        segmentIdx + ",state=" + initState.index() + ']');
                }

                // WAL segment is a hard link to a segment file in the special CDC folder.
                // So, we can safely delete it after processing.
                Files.delete(segment);
            }
            else {
                builder.from(initState);

                initState = null;
            }
        }

        try (WALIterator it = factory.iterator(builder)) {
            while (it.hasNext()) {
                boolean commit;

                synchronized (this) {
                    commit = consumer.onRecord(it.next().get2());
                }

                if (commit) {
                    assert it.lastRead().isPresent();

                    state.save(it.lastRead().get());

                    // Can delete after new file state save.
                    if (!prevSegments.isEmpty()) {
                        // WAL segment is a hard link to a segment file in a specifal CDC folder.
                        // So we can safely delete it after success processing.
                        for (Path prevSegment : prevSegments)
                            Files.deleteIfExists(prevSegment);

                        prevSegments.clear();
                    }
                }
            }
        }

        prevSegments.add(segment);
    }

    /**
     * @param workDir Working directory.
     * @return WAL archive directory.
     */
    private Path findCDCDir(String workDir) throws InterruptedException {
        Path parent;

        if (cfg.getDataStorageConfiguration() != null &&
            !F.isEmpty(cfg.getDataStorageConfiguration().getCdcPath())) {
            parent = Paths.get(cfg.getDataStorageConfiguration().getCdcPath());

            if (!parent.isAbsolute())
                parent = Paths.get(workDir, cfg.getDataStorageConfiguration().getCdcPath());
        }
        else
            parent = Paths.get(workDir).resolve(DFLT_CDC_PATH);

        log.info("CDC root[dir=" + parent + ']');

        final Path[] cdcDir = new Path[1];

        String nodePattern = nodeDir == null ? (NODE_PATTERN + UUID_STR_PATTERN) : nodeDir;

        log.info("ConsistendId pattern[dir=" + nodePattern + ']');

        waitFor(parent,
            dir -> dir.getName(dir.getNameCount() - 1).toString().matches(nodePattern),
            Path::compareTo,
            dir -> {
                cdcDir[0] = dir;

                return false;
            }, log
        );

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

            IgniteLogger log = IgnitionEx.IgniteNamedInstance.initLogger(cfg.getGridLogger(), appLogId, workDir);

            log.info("App Log ID     -\t" + appLogId);

            return log;
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

    /**
     * @param segment WAL segment file.
     * @return Segment index.
     */
    public long segmentNumber(Path segment) {
        String fn = segment.getFileName().toString();

        return Long.parseLong(fn.substring(0, fn.indexOf('.')));
    }

    /**
     * @param cfg Configuration.
     * @return Consistent id to use.
     */
    private String consistentId(IgniteConfiguration cfg) {
        if (cfg.getConsistentId() != null)
            return U.maskForFileName(cfg.getConsistentId().toString());

        String consistendId = IgniteSystemProperties.getString(IGNITE_CDC_CONSISTENT_ID, null);

        if (consistendId == null) {
            log.warning(IGNITE_CDC_CONSISTENT_ID + " is null.");

            return null;
        }

        int idx = IgniteSystemProperties.getInteger(IGNITE_CDC_NODE_IDX, -1);

        if (idx == -1) {
            log.warning(IGNITE_CDC_NODE_IDX + " is null.");

            return null;
        }

        return PdsConsistentIdProcessor.genNewStyleSubfolderName(idx, UUID.fromString(consistendId));
    }

    /**
     * Waits for the files or directories to be created insied {@code watchDir}
     * and if new file pass the {@code filter} then {@code callback} notified with the newly create file.
     * {@code callback} will allso be notified about already existing files that passes the filter.
     *
     * @param watchDir Directory to watch.
     * @param filter Filter of events.
     * @param existingSorter Sorter of existing files.
     * @param callback Callback to be notified.
     */
    public static void waitFor(Path watchDir, Predicate<Path> filter, Comparator<Path> existingSorter,
        Predicate<Path> callback, IgniteLogger log) throws InterruptedException {
        // If watch dir not exists waiting for it creation.
        if (!Files.exists(watchDir))
            waitFor(watchDir.getParent(), watchDir::equals, Path::compareTo, p -> false, log);

        try {
            try (WatchService watcher = FileSystems.getDefault().newWatchService()) {
                watchDir.register(watcher, ENTRY_CREATE, ENTRY_DELETE);

                Set<Path> seen = new HashSet<>();

                try (Stream<Path> children = Files.walk(watchDir, 1).filter(p -> !p.equals(watchDir))) {
                    final boolean[] status = {true};

                    children
                        .filter(filter)
                        .sorted()
                        .peek(seen::add)
                        .peek(p -> {
                            if (status[0])
                                status[0] = callback.test(p);
                        }).count();

                    if (!status[0])
                        return;
                }

                if (log.isDebugEnabled())
                    log.debug("Waiting for creation of child directory. Watching directory[dir=" + watchDir + ']');

                boolean needNext = true;

                while (needNext) {
                    WatchKey key = watcher.take();

                    for (WatchEvent<?> evt: key.pollEvents()) {
                        WatchEvent.Kind<?> kind = evt.kind();

                        Path evtPath = Paths.get(watchDir.toString(), evt.context().toString()).toAbsolutePath();

                        if (kind == ENTRY_DELETE)
                            seen.remove(evtPath);
                        else if (kind == ENTRY_CREATE) {
                            if (seen.contains(evtPath))
                                continue;

                            if (log.isDebugEnabled())
                                log.debug("Event received[evt=" + evtPath.toAbsolutePath() + ",kind=" + kind + ']');

                            if (!filter.test(evtPath))
                                continue;

                            needNext = callback.test(evtPath);

                            if (!needNext)
                                break;
                        }
                    }

                    if (!needNext)
                        break;

                    boolean reset = key.reset();

                    if (!reset)
                        throw new IllegalStateException("Key no longer valid.");
                }
            }
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }
}
