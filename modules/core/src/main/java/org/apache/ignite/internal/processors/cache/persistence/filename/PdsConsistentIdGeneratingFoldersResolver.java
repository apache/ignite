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
import java.io.FileFilter;
import java.io.Serializable;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Component for resolving PDS storage file names, also used for generating consistent ID for case PDS mode is enabled
 */
public class PdsConsistentIdGeneratingFoldersResolver extends GridProcessorAdapter implements PdsFolderResolver {
    /** Database subfolders constant prefix. */
    public static final String DB_FOLDER_PREFIX = "Node";
    public static final String NODEIDX_UID_SEPARATOR = "-";

    public static final String NODE_PATTERN = DB_FOLDER_PREFIX + "[0-9]*" + NODEIDX_UID_SEPARATOR;
    public static final String UUID_STR_PATTERN = "[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}";

    public static final String SUBDIR_PATTERN = NODE_PATTERN + UUID_STR_PATTERN;
    public static final FileFilter DB_SUBFOLDERS_NEW_STYLE_FILTER = new FileFilter() {
        @Override public boolean accept(File pathname) {
            return pathname.isDirectory() && pathname.getName().matches(SUBDIR_PATTERN);
        }
    };

    /** Database default folder. */
    public static final String DB_DEFAULT_FOLDER = "db";

    private IgniteConfiguration cfg;
    private GridDiscoveryManager discovery;
    /** Logger. */
    private IgniteLogger log;
    private GridKernalContext ctx;

    /** Cached folder settings. */
    private PdsFolderSettings settings;

    public PdsConsistentIdGeneratingFoldersResolver(GridKernalContext ctx) {
        super(ctx);
        this.cfg = ctx.config();
        this.discovery = ctx.discovery();
        this.log = ctx.log(PdsFolderResolver.class);
        this.ctx = ctx;
    }

    public PdsFolderSettings compatibleResolve() {
        return new PdsFolderSettings(discovery.consistentId(), true);
    }

    public PdsFolderSettings resolveFolders() throws IgniteCheckedException {
        if (settings == null) {
            settings = prepareNewSettings();

            if (!settings.isCompatible()) {
                //todo are there any other way to set this value?
                cfg.setConsistentId(settings.consistentId());
            }
        }
        return settings;
    }

    private PdsFolderSettings prepareNewSettings() throws IgniteCheckedException {
        if (!cfg.isPersistentStoreEnabled())
            return compatibleResolve();

        // compatible mode from configuration is used fot this case
        if (cfg.getConsistentId() != null) {
            // compatible mode from configuration is used fot this case, no locking, no consitent id change
            return new PdsFolderSettings(cfg.getConsistentId(), true);
        }
        // The node scans the work directory and checks if there is a folder matching the consistent ID. If such a folder exists, we start up with this ID (compatibility mode)

        final File pstStoreBasePath = resolvePersistentStoreBasePath();

        // this is required to correctly initialize SPI
        final DiscoverySpi spi = discovery.tryInjectSpi();
        if (spi instanceof TcpDiscoverySpi) {
            final TcpDiscoverySpi tcpDiscoverySpi = (TcpDiscoverySpi)spi;
            final String oldStyleConsistentId = tcpDiscoverySpi.calculateConsistentIdAddrPortBased();
            final String subFolder = U.maskForFileName(oldStyleConsistentId);

            final GridCacheDatabaseSharedManager.FileLockHolder fileLockHolder = tryLock(new File(pstStoreBasePath, subFolder));
            if (fileLockHolder != null)
                return new PdsFolderSettings(oldStyleConsistentId, subFolder, -1, fileLockHolder, false);

        }

        FolderCandidate lastCheckedCandidate = null;
        FolderCandidate listedCandidate;
        int minNodeIdxToCheck = 0;
        while ((listedCandidate = findLowestIndexInFolder(pstStoreBasePath, minNodeIdxToCheck)) != null) {
            //already have such directory here
            final GridCacheDatabaseSharedManager.FileLockHolder fileLockHolder = tryLock(listedCandidate.file);
            if (fileLockHolder != null) {
                //todo remove debug output
                System.out.println("locked>> " + pstStoreBasePath + " " + listedCandidate.file);
                return new PdsFolderSettings(listedCandidate.uuid(), listedCandidate.file.getName(), listedCandidate.nodeIndex(), fileLockHolder, false);
            }
            //lock was not successful
            lastCheckedCandidate = listedCandidate;
            minNodeIdxToCheck = listedCandidate.nodeIndex() + 1;
        }

        // was not able to find free slot, allocating new
        int nodeIdx = lastCheckedCandidate == null ? 0 : lastCheckedCandidate.nodeIndex() + 1;
        UUID uuid = UUID.randomUUID();
        String uuidAsStr = uuid.toString();

        assert uuidAsStr.matches(UUID_STR_PATTERN);

        String consIdFolderReplacement = DB_FOLDER_PREFIX + Integer.toString(nodeIdx) + NODEIDX_UID_SEPARATOR + uuidAsStr;

        final File newRandomFolder = U.resolveWorkDirectory(pstStoreBasePath.getAbsolutePath(), consIdFolderReplacement, false); //mkdir here
        final GridCacheDatabaseSharedManager.FileLockHolder fileLockHolder = tryLock(newRandomFolder);
        if (fileLockHolder != null) {
            //todo remove debug output
            System.out.println("locked>> " + pstStoreBasePath + " " + consIdFolderReplacement);
            return new PdsFolderSettings(uuid, consIdFolderReplacement, nodeIdx, fileLockHolder, false);
        }
        throw new IgniteCheckedException("Unable to lock file generated randomly [" + newRandomFolder + "]");
    }

    /**
     * @param pstStoreBasePath root storage folder to scan
     * @param minNodeIdxToCheck minimal index can be next candidate, 0-based (inclusive)
     * @return null if there is no files in folder to test, or all files has lower index than bound {@code
     * minNodeIdxToCheck}. Non null value is returned for folder having lowest node index
     */
    @Nullable private FolderCandidate findLowestIndexInFolder(File pstStoreBasePath, int minNodeIdxToCheck) {
        FolderCandidate candidate = null;
        for (File file : pstStoreBasePath.listFiles(DB_SUBFOLDERS_NEW_STYLE_FILTER)) {
            final NodeIndexAndUid nodeIdxAndUid = parseFileName(file);
            if (nodeIdxAndUid == null)
                continue;

            //filter out this folder because lock on such low candidate was alrady checked
            if (nodeIdxAndUid.nodeIndex() < minNodeIdxToCheck)
                continue;

            if (candidate == null) {
                candidate = new FolderCandidate(file, nodeIdxAndUid);
                continue;
            }

            if (nodeIdxAndUid.nodeIndex() < candidate.nodeIndex())
                candidate = new FolderCandidate(file, nodeIdxAndUid);
        }
        return candidate;
    }

    /**
     * Tries to lock subfolder within storage root folder
     *
     * @param dbStoreDirWithSubdirectory DB store directory, is to be absolute and should include consistent ID based
     * sub folder
     * @return non null holder if lock was successful, null in case lock failed. If directory does not exist method will
     * always fail to lock.
     */
    private GridCacheDatabaseSharedManager.FileLockHolder tryLock(File dbStoreDirWithSubdirectory) {
        if (!dbStoreDirWithSubdirectory.exists())
            return null;
        final String path = dbStoreDirWithSubdirectory.getAbsolutePath();
        final GridCacheDatabaseSharedManager.FileLockHolder fileLockHolder
            = new GridCacheDatabaseSharedManager.FileLockHolder(path, ctx, log);
        try {
            fileLockHolder.tryLock(1000);
            return fileLockHolder;
        }
        catch (IgniteCheckedException e) {
            U.closeQuiet(fileLockHolder);
            log.info("Unable to acquire lock to file [" + path + "], reason: " + e.getMessage());
            return null;
        }
    }

    private File resolvePersistentStoreBasePath() throws IgniteCheckedException {
        final PersistentStoreConfiguration pstCfg = cfg.getPersistentStoreConfiguration();

        final File dirToFindOldConsIds;
        if (pstCfg.getPersistentStorePath() != null) {
            File workDir0 = new File(pstCfg.getPersistentStorePath());

            if (!workDir0.isAbsolute())
                dirToFindOldConsIds = U.resolveWorkDirectory(
                    cfg.getWorkDirectory(),
                    pstCfg.getPersistentStorePath(),
                    false
                );
            else
                dirToFindOldConsIds = workDir0;
        }
        else {
            dirToFindOldConsIds = U.resolveWorkDirectory(
                cfg.getWorkDirectory(),
                DB_DEFAULT_FOLDER,
                false
            );
        }
        return dirToFindOldConsIds;
    }

    /**
     * @param subFolderFile new style folder name to parse
     * @return Pair of UUID and node index
     */
    private NodeIndexAndUid parseFileName(@NotNull final File subFolderFile) {
        return parseSubFolderName(subFolderFile, log);
    }

    /**
     * @param file new style file to parse.
     * @param log Logger.
     * @return Pair of UUID and node index.
     */
    @Nullable public static NodeIndexAndUid parseSubFolderName(
        @NotNull File file, IgniteLogger log) {
        final String fileName = file.getName();
        Matcher m = Pattern.compile(NODE_PATTERN).matcher(fileName);
        if (!m.find())
            return null;
        int uidStart = m.end();
        try {
            String uid = fileName.substring(uidStart);
            final UUID uuid = UUID.fromString(uid);
            final String substring = fileName.substring(DB_FOLDER_PREFIX.length(), uidStart - NODEIDX_UID_SEPARATOR.length());
            final int idx = Integer.parseInt(substring);
            return new NodeIndexAndUid(idx, uuid);
        }
        catch (Exception e) {
            log.warning("Unable to parse new style file format: " + e);
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        if (settings != null) {
            final GridCacheDatabaseSharedManager.FileLockHolder fileLockHolder = settings.takeLockedFileLockHolder();
            if (fileLockHolder != null) {
                fileLockHolder.release();
                fileLockHolder.close();
            }
        }
        super.stop(cancel);
    }

    public static class FolderCandidate {

        private final File file;
        private final NodeIndexAndUid params;

        public FolderCandidate(File file, NodeIndexAndUid params) {

            this.file = file;
            this.params = params;
        }

        public int nodeIndex() {
            return params.nodeIndex();
        }

        public Serializable uuid() {
            return params.uuid();
        }
    }

    public static class NodeIndexAndUid {
        private final int nodeIndex;
        private final UUID uuid;

        public NodeIndexAndUid(int nodeIndex, UUID nodeConsistentId) {
            this.nodeIndex = nodeIndex;
            this.uuid = nodeConsistentId;
        }

        public int nodeIndex() {
            return nodeIndex;
        }

        public Serializable uuid() {
            return uuid;
        }
    }

}


