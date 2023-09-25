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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMetadata;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.Dump;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.Dump.DumpedPartitionIterator;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_MARSHALLER_PATH;

/**
 * Dump Reader application.
 * The application runs independently of Ignite node process and provides the ability to the {@link DumpConsumer} to consume
 * all data stored in cache dump ({@link Dump})
 */
public class DumpReader implements Runnable {
    /** Configuration. */
    private final DumpReaderConfiguration cfg;

    /** */
    private final GridKernalContext kctx;

    /**
     * @param cfg Dump reader configuration.
     * @param kctx Kernal context.
     */
    public DumpReader(DumpReaderConfiguration cfg, GridKernalContext kctx) {
        this.cfg = cfg;
        this.kctx = kctx;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        try {
            Dump dump = new Dump(kctx, cfg.dumpRoot());
            DumpConsumer cnsmr = cfg.consumer();

            cnsmr.start();

            try {
                File[] files = new File(cfg.dumpRoot(), DFLT_MARSHALLER_PATH).listFiles(BinaryUtils::notTmpFile);

                if (files != null)
                    cnsmr.onMappings(CdcMain.typeMappingIterator(files, tm -> true));

                cnsmr.onTypes(kctx.cacheObjects().metadata().iterator());

                Map<Integer, List<String>> grpToNodes = new HashMap<>();

                for (SnapshotMetadata meta : dump.metadata()) {
                    for (Integer grp : meta.cacheGroupIds())
                        grpToNodes.computeIfAbsent(grp, key -> new ArrayList<>()).add(meta.folderName());
                }

                cnsmr.onCacheConfigs(grpToNodes.entrySet().stream()
                    .flatMap(e -> dump.configs(e.getValue().get(0), e.getKey()).stream())
                    .iterator());

                ExecutorService execSvc = Executors.newFixedThreadPool(cfg.threadCount());

                IgniteLogger log = kctx.log(DumpReader.class);
                AtomicBoolean skip = new AtomicBoolean(false);

                for (Map.Entry<Integer, List<String>> e : grpToNodes.entrySet()) {
                    int grp = e.getKey();

                    for (String node : e.getValue()) {
                        for (int part : dump.partitions(node, grp)) {
                            execSvc.submit(() -> {
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

                                    throw new IgniteException(ex);
                                }
                            });
                        }
                    }
                }

                execSvc.shutdown();

                execSvc.awaitTermination(cfg.timeout().toMillis(), MILLISECONDS);
            }
            finally {
                cnsmr.stop();
            }
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }
}
