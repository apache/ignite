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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
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
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.encryption.EncryptionSpi;
import org.jetbrains.annotations.Nullable;

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
    /** Configuration. */
    private final DumpReaderConfiguration cfg;

    /** Log. */
    protected final IgniteLogger log;

    /** */
    protected final Map<Integer, List<String>> grpToNodes = new HashMap<>();

    /** */
    @Nullable protected final Map<Integer, String> cacheGrpIds;

    /** */
    protected Dump dump;

    /** */
    protected DumpConsumer cnsmr;

    /** */
    protected final ExecutorService execSvc;

    /**
     * @param cfg Dump reader configuration.
     * @param log Logger.
     */
    public DumpReader(DumpReaderConfiguration cfg, IgniteLogger log) {
        this.cfg = cfg;
        this.log = log.getLogger(DumpReader.class);

        this.cacheGrpIds = cfg.cacheGroupNames() != null
            ? Arrays.stream(cfg.cacheGroupNames()).collect(Collectors.toMap(CU::cacheId, Function.identity()))
            : null;

        this.execSvc = Executors.newFixedThreadPool(Math.min(1, cfg.threadCount()));
    }

    /** {@inheritDoc} */
    @Override public void run() {
        ackAsciiLogo();

        try (Dump dump = new Dump(cfg.dumpRoot(), null, cfg.keepBinary(), false, encryptionSpi(), log)) {
            this.dump = dump;
            this.cnsmr = cfg.consumer();

            cnsmr.start();

            try {
                dump.metadata().forEach(this::onMeta);

                File[] files = new File(cfg.dumpRoot(), DFLT_MARSHALLER_PATH).listFiles(BinaryUtils::notTmpFile);

                if (files != null)
                    cnsmr.onMappings(CdcMain.typeMappingIterator(files, tm -> true));

                cnsmr.onTypes(dump.types());

                cnsmr.onCacheConfigs(grpToNodes.entrySet().stream()
                    .flatMap(e -> dump.configs(F.first(e.getValue()), e.getKey()).stream())
                    .iterator());

                AtomicBoolean skip = new AtomicBoolean(false);

                Map<Integer, Set<Integer>> grps = cfg.skipCopies() ? new HashMap<>() : null;

                if (grps != null)
                    grpToNodes.keySet().forEach(grpId -> grps.put(grpId, new HashSet<>()));

                AtomicReference<Exception> err = new AtomicReference<>();

                for (Map.Entry<Integer, List<String>> e : grpToNodes.entrySet()) {
                    int grp = e.getKey();

                    for (String node : e.getValue()) {
                        for (int part : dump.partitions(node, grp)) {
                            if (grps != null && !grps.get(grp).add(part)) {
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

                                try (DumpedPartitionIterator iter = dump.iterator(node, grp, part)) {
                                    if (log.isDebugEnabled()) {
                                        log.debug("Consuming partition [node=" + node + ", grp=" + grp +
                                            ", part=" + part + ']');
                                    }

                                    cnsmr.onPartition(grp, part, iter);
                                }
                                catch (Exception ex) {
                                    skip.set(cfg.failFast());

                                    log.error("Error consuming partition [node=" + node + ", grp=" + grp +
                                        ", part=" + part + ']', ex);

                                    err.compareAndSet(null, new IgniteException(ex));
                                }
                            };

                            execSvc.submit(consumePart);
                        }
                    }
                }

                execSvc.shutdown();

                boolean res = execSvc.awaitTermination(cfg.timeout().toMillis(), MILLISECONDS);

                if (err.get() != null)
                    throw err.get();

                if (!res) {
                    log.warning("Dump processing tasks not finished after timeout. Cancelling");

                    execSvc.shutdownNow();
                }
            }
            finally {
                try {
                    cnsmr.stop();
                }
                finally {
                    this.dump = null;
                    this.cnsmr = null;

                    this.execSvc.shutdown();
                }
            }
        }
        catch (Exception e) {
            throw e instanceof IgniteException ? (IgniteException)e : new IgniteException("Failed to run dump reader.", e);
        }
    }

    /** */
    protected void onMeta(SnapshotMetadata meta) {
        for (Integer grp : meta.cacheGroupIds()) {
            if (cacheGrpIds == null || cacheGrpIds.keySet().contains(grp))
                grpToNodes.computeIfAbsent(grp, key -> new ArrayList<>()).add(meta.folderName());
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

            U.quiet(false,
                "  ^-- To see **FULL** console log here add -DIGNITE_QUIET=false or \"-v\" to ignite-cdc.{sh|bat}",
                "");
        }
    }

    /** @return EncryptionSpi. */
    public EncryptionSpi encryptionSpi() {
        EncryptionSpi encSpi = cfg.encryptionSpi();

        if (encSpi == null)
            return null;

        if (encSpi instanceof IgniteSpiAdapter)
            ((IgniteSpiAdapter)encSpi).onBeforeStart();

        encSpi.spiStart("dump-reader");

        return encSpi;
    }
}
