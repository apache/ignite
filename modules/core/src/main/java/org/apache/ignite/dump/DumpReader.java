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

package org.apache.ignite.dump;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridLoggerProxy;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMetadata;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.Dump;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.Dump.DumpedPartitionIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteExperimental;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_MARSHALLER_PATH;
import static org.apache.ignite.internal.IgniteKernal.NL;
import static org.apache.ignite.internal.IgniteKernal.SITE;
import static org.apache.ignite.internal.IgniteVersionUtils.ACK_VER_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;

/**
 * Dump Reader application.
 * The application runs independently of Ignite node process and provides the ability to the {@link DumpConsumer} to consume
 * all data stored in cache dump ({@link Dump})
 */
@IgniteExperimental
public class DumpReader implements Runnable {
    /** Update rate stats print period. */
    private static final int UPDATE_RATE_STATS_PRINT_PERIOD_SECONDS = 30;

    /** Configuration. */
    private final DumpReaderConfiguration cfg;

    /** Log. */
    private final IgniteLogger log;

    /**
     * @param cfg Dump reader configuration.
     * @param log Logger.
     */
    public DumpReader(DumpReaderConfiguration cfg, IgniteLogger log) {
        this.cfg = cfg;
        this.log = log.getLogger(DumpReader.class);
    }

    /** {@inheritDoc} */
    @Override public void run() {
        ackAsciiLogo();

        try (Dump dump = new Dump(cfg.dumpRoot(), cfg.keepBinary(), false, log)) {
            DumpConsumer cnsmr = cfg.consumer();

            cnsmr.start();

            try (StatsLog statsLog = new StatsLog()) {
                File[] files = new File(cfg.dumpRoot(), DFLT_MARSHALLER_PATH).listFiles(BinaryUtils::notTmpFile);

                if (files != null)
                    cnsmr.onMappings(CdcMain.typeMappingIterator(files, tm -> true));

                cnsmr.onTypes(dump.types());

                Map<Integer, List<String>> grpToNodes = new HashMap<>();

                Set<Integer> cacheGroupIds = cfg.cacheGroupNames() != null 
                    ? Arrays.stream(cfg.cacheGroupNames()).map(CU::cacheId).collect(Collectors.toSet())
                    : null;

                for (SnapshotMetadata meta : dump.metadata()) {
                    for (Integer grp : meta.cacheGroupIds()) {
                        if (cacheGroupIds == null || cacheGroupIds.contains(grp))
                            grpToNodes.computeIfAbsent(grp, key -> new ArrayList<>()).add(meta.folderName());
                    }
                }

                cnsmr.onCacheConfigs(grpToNodes.entrySet().stream()
                    .flatMap(e -> dump.configs(F.first(e.getValue()), e.getKey()).stream())
                    .iterator());

                ExecutorService execSvc = cfg.threadCount() > 1 ? Executors.newFixedThreadPool(cfg.threadCount()) : null;

                AtomicBoolean skip = new AtomicBoolean(false);

                int partsCnt = grpToNodes.entrySet().stream()
                    .mapToInt(e -> e.getValue().stream()
                        .map(node -> dump.partitions(node, e.getKey()))
                        .flatMap(Collection::stream)
                        .collect(Collectors.toCollection(cfg.skipCopies() ? HashSet::new : ArrayList::new))
                        .size())
                    .sum();

                AtomicInteger partsProcessed = new AtomicInteger(0);

                Map<Integer, Set<Integer>> groups = cfg.skipCopies() ? new HashMap<>() : null;

                for (Map.Entry<Integer, List<String>> e : grpToNodes.entrySet()) {
                    int grp = e.getKey();

                    for (String node : e.getValue()) {
                        for (int part : dump.partitions(node, grp)) {
                            if (groups != null && !groups.computeIfAbsent(grp, x -> new HashSet<>()).add(part)) {
                                log.info("Skip copy partition [node=" + node + ", grp=" + grp + ", part=" + part + ']');

                                continue;
                            }

                            Runnable consumePart = () -> {
                                if (skip.get()) {
                                    if (log.isDebugEnabled()) {
                                        log.debug("Skip partition due to previous error [node=" + node + ", grp=" + grp +
                                            ", part=" + part + ']');
                                    }

                                    return;
                                }

                                try (DumpedPartitionIterator iter = new DumpedPartitionIterator() {
                                    /** */
                                    final DumpedPartitionIterator delegate = dump.iterator(node, grp, part);

                                    /** */
                                    final AtomicBoolean consumerProcessingEntry = new AtomicBoolean(false);

                                    /** {@inheritDoc } */
                                    @Override public boolean hasNext() {
                                        if (consumerProcessingEntry.compareAndSet(true, false))
                                            statsLog.recordProcessed();

                                        return delegate.hasNext();
                                    }

                                    /** {@inheritDoc } */
                                    @Override public DumpEntry next() {
                                        if (consumerProcessingEntry.compareAndSet(true, false)) {
                                            // Consumer didn't execute hasNext()
                                            statsLog.recordProcessed();
                                        }

                                        DumpEntry next = delegate.next();

                                        consumerProcessingEntry.set(true);

                                        return next;
                                    }

                                    /** {@inheritDoc } */
                                    @Override public void close() throws Exception {
                                        delegate.close();
                                    }
                                }) {
                                    if (log.isDebugEnabled()) {
                                        log.debug("Consuming partition [node=" + node + ", grp=" + grp +
                                            ", part=" + part + ']');
                                    }

                                    cnsmr.onPartition(grp, part, iter);

                                    int partNo = partsProcessed.incrementAndGet();

                                    if (log.isDebugEnabled())
                                        log.debug("Consumed partitions " + partNo + " of " + partsCnt);
                                }
                                catch (Exception ex) {
                                    skip.set(cfg.failFast());

                                    log.error("Error consuming partition [node=" + node + ", grp=" + grp +
                                        ", part=" + part + ']', ex);

                                    throw new IgniteException(ex);
                                }
                            };

                            if (cfg.threadCount() > 1)
                                execSvc.submit(consumePart);
                            else
                                consumePart.run();
                        }
                    }
                }

                if (cfg.threadCount() > 1) {
                    execSvc.shutdown();

                    boolean res = execSvc.awaitTermination(cfg.timeout().toMillis(), MILLISECONDS);

                    if (!res) {
                        log.warning("Dump processing tasks not finished after timeout. Cancelling");

                        execSvc.shutdownNow();
                    }
                }
            }
            finally {
                cnsmr.stop();
            }
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /** */
    private void ackAsciiLogo() {
        String ver = "ver. " + ACK_VER_STR;

        if (log.isInfoEnabled()) {
            log.info(NL + NL +
                ">>>    __________  ________________  ___  __  ____  ______    ___  _______   ___  _______" + NL +
                ">>>   /  _/ ___/ |/ /  _/_  __/ __/ / _ \\/ / / /  |/  / _ \\  / _ \\/ __/ _ | / _ \\/ __/ _ \\" + NL +
                ">>>  _/ // (_ /    // /  / / / _/  / // / /_/ / /|_/ / ___/ / , _/ _// __ |/ // / _// , _/" + NL +
                ">>> /___/\\___/_/|_/___/ /_/ /___/ /____/\\____/_/  /_/_/    /_/|_/___/_/ |_/____/___/_/|_|" + NL +
                ">>> " + NL +
                ">>> " + ver + NL +
                ">>> " + COPYRIGHT + NL +
                ">>> " + NL +
                ">>> Ignite documentation: " + "http://" + SITE + NL +
                ">>> ConsistentId: " + cfg.dumpRoot() + NL +
                ">>> Consumer: " + U.toStringSafe(cfg.consumer())
            );
        }

        if (log.isQuiet()) {
            U.quiet(false,
                "   __________  ________________  ___  __  ____  ______    ___  _______   ___  _______",
                "  /  _/ ___/ |/ /  _/_  __/ __/ / _ \\/ / / /  |/  / _ \\  / _ \\/ __/ _ | / _ \\/ __/ _ \\",
                " _/ // (_ /    // /  / / / _/  / // / /_/ / /|_/ / ___/ / , _/ _// __ |/ // / _// , _/",
                "/___/\\___/_/|_/___/ /_/ /___/ /____/\\____/_/  /_/_/    /_/|_/___/_/ |_/____/___/_/|_|",
                "",
                ver,
                COPYRIGHT,
                "",
                "Ignite documentation: " + "http://" + SITE,
                "Dump: " + cfg.dumpRoot(),
                "Consumer: " + U.toStringSafe(cfg.consumer()),
                "",
                "Quiet mode.");

            String fileName = log.fileName();

            if (fileName != null)
                U.quiet(false, "  ^-- Logging to file '" + fileName + '\'');

            if (log instanceof GridLoggerProxy)
                U.quiet(false, "  ^-- Logging by '" + ((GridLoggerProxy)log).getLoggerInfo() + '\'');
        }
    }

    /** */
    private class StatsLog implements AutoCloseable {
        /** */
        private final LongAdder recordsProcessed = new LongAdder();

        /** */
        private final long startTime = System.currentTimeMillis();

        /** */
        private long lastLogTime = startTime;

        /** */
        private long lastObservedCnt;

        /** */
        private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        /** */
        private StatsLog() {
            scheduler.scheduleAtFixedRate(() -> {
                    long now = System.currentTimeMillis();
                    long cnt = recordsProcessed.longValue();

                    long newRecs = cnt - lastObservedCnt;
                    long currentRate = 1000L * newRecs / (now - lastLogTime);
                    long avgRate = 1000L * cnt / (now - startTime);

                    log.info(">>>" +
                        "\n>>> Records processed stats" +
                        "\n>>>   totalRecords: " +
                        cnt +
                        "\n>>>   newRecords: " +
                        newRecs +
                        "\n>>>   avgRate: " +
                        avgRate +
                        "\n>>>   currentRate: " +
                        currentRate
                    );

                    lastLogTime = now;
                    lastObservedCnt = cnt;
                },
                UPDATE_RATE_STATS_PRINT_PERIOD_SECONDS,
                UPDATE_RATE_STATS_PRINT_PERIOD_SECONDS,
                TimeUnit.SECONDS
            );
        }

        /** */
        private void recordProcessed() {
            recordsProcessed.increment();
        }

        /** {@inheritDoc} */
        @Override public void close() {
            scheduler.shutdownNow();
        }

    }
}
