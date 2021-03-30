/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.filename;

import java.io.File;
import java.io.Serializable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.NodeFileLockHolder;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Component for resolving PDS storage file names, also used for generating consistent ID for case PDS mode is enabled
 */
public class PdsConsistentIdProcessor extends GridProcessorAdapter implements PdsFoldersResolver {
    /** Config. */
    private final IgniteConfiguration cfg;

    /** Logger. */
    private final IgniteLogger log;

    /** Context. */
    private final GridKernalContext ctx;

    /** Cached folder settings. */
    private PdsFolderSettings<NodeFileLockHolder> settings;

    /**
     * Creates folders resolver
     *
     * @param ctx Context.
     */
    public PdsConsistentIdProcessor(final GridKernalContext ctx) {
        super(ctx);

        this.cfg = ctx.config();
        this.log = ctx.log(PdsFoldersResolver.class);
        this.ctx = ctx;
    }

    /**
     * Prepares compatible PDS folder settings. No locking is performed, consistent ID is not overridden.
     *
     * @param pstStoreBasePath DB storage base path or null if persistence is not enabled.
     * @param consistentId compatibility consistent ID
     * @return PDS folder settings compatible with previous versions.
     */
    private PdsFolderSettings<NodeFileLockHolder> compatibleResolve(
        @Nullable final File pstStoreBasePath,
        @NotNull final Serializable consistentId) {

        if (cfg.getConsistentId() != null) {
            // compatible mode from configuration is used fot this case, no locking, no consitent id change
            return new PdsFolderSettings<>(pstStoreBasePath, cfg.getConsistentId());
        }

        return new PdsFolderSettings<>(pstStoreBasePath, consistentId);
    }

    /** {@inheritDoc} */
    @Override public PdsFolderSettings<NodeFileLockHolder> resolveFolders() throws IgniteCheckedException {
        if (settings == null) {
            PdsFolderResolver<NodeFileLockHolder> resolver =
                new PdsFolderResolver<>(cfg, log, ctx.discovery().consistentId(), this::tryLock);

            settings = resolver.resolve();

            if (settings == null)
                settings = resolver.generateNew();

            if (!settings.isCompatible()) {
                if (log.isInfoEnabled())
                    log.info("Consistent ID used for local node is [" + settings.consistentId() + "] " +
                        "according to persistence data storage folders");

                ctx.discovery().consistentId(settings.consistentId());
            }
        }
        return settings;
    }

    /**
     * Tries to lock subfolder within storage root folder.
     *
     * @param dbStoreDirWithSubdirectory DB store directory, is to be absolute and should include consistent ID based
     * sub folder.
     * @return non null holder if lock was successful, null in case lock failed. If directory does not exist method will
     * always fail to lock.
     */
    private NodeFileLockHolder tryLock(File dbStoreDirWithSubdirectory) {
        if (!dbStoreDirWithSubdirectory.exists())
            return null;

        final String path = dbStoreDirWithSubdirectory.getAbsolutePath();
        final NodeFileLockHolder fileLockHolder
            = new NodeFileLockHolder(path, ctx, log);

        try {
            fileLockHolder.tryLock(1000);

            return fileLockHolder;
        }
        catch (IgniteCheckedException e) {
            U.closeQuiet(fileLockHolder);

            if (log.isInfoEnabled())
                log.info("Unable to acquire lock to file [" + path + "], reason: " + e.getMessage());

            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        if (settings != null) {
            final NodeFileLockHolder fileLockHolder = settings.getLockedFileLockHolder();

            if (fileLockHolder != null)
                fileLockHolder.close();
        }

        super.stop(cancel);
    }
}


