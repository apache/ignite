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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

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

    /**
     * @param cfg Ignite configuration.
     * @param consumer Event consumer.
     */
    public IgniteCDC(IgniteConfiguration cfg, CDCConsumer consumer) {
        this.cfg = cfg;
        this.consumer = consumer;
        this.log = cfg.getGridLogger().getLogger(IgniteCDC.class);
    }

    /** {@inheritDoc} */
    @Override public void run() {
        if (log.isInfoEnabled()) {
            log.info("Starting Ignite CDC Application.");
            log.info(consumer.toString());
            log.info("--------------------------------");
        }

        File wal = findWal();

        if (log.isInfoEnabled())
            log.info("Found wal dir[dir=" + wal + ']');

        consumer.start(cfg);
    }

    private File findWal() {
        try {
            if (log.isDebugEnabled())
                log.debug("Searching wal directory");

            File workDir = U.resolveWorkDirectory(cfg.getWorkDirectory(), DataStorageConfiguration.DFLT_WAL_PATH,
                false, false);

            if (!workDir.exists())
                log.error("Work dir not found![dir=" + workDir + "]");

            File[] nodeWalDirs = workDir.listFiles(f ->
                !f.getAbsolutePath().contains(DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH));

            if (F.isEmpty(nodeWalDirs))
                throw new IllegalStateException("Can't find wal directories[workDir=" + workDir.getAbsolutePath() + ']');

            if (log.isDebugEnabled()) {
                for (File walDir : nodeWalDirs)
                    log.debug("Found wal dir[dir=" + walDir + ']');
            }

            if (nodeWalDirs.length == 1)
                return nodeWalDirs[0];
            else if (cfg.getConsistentId() == null)
                throw new IllegalStateException("Found several WAL dirs but no consistentId provided in config");

            String consistentId = U.maskForFileName(cfg.getConsistentId().toString());

            for (File dir : nodeWalDirs) {
                if (log.isInfoEnabled())
                    log.info(dir.getName());
                if (dir.getName().equalsIgnoreCase(consistentId))
                    return dir;
            }

            throw new IllegalStateException("WAL dir for consistentId not found[consistenId=" + consistentId + ']');
        }
        catch (IgniteCheckedException e) {
            log.error("Can't find wal directory.", e);

            throw new IgniteException(e);
        }
    }
}
