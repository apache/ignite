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
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DATA_STORAGE_FOLDER_BY_CONSISTENT_ID;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;

/**
 * Component for resolving PDS storage file names, also used for generating consistent ID for case PDS mode is enabled
 */
public class PdsConsistentIdProcessor extends GridProcessorAdapter implements PdsFoldersResolver {
    /** Database subfolders constant prefix. */
    private static final String DB_FOLDER_PREFIX = "node";

    /** Node index and uid separator in subfolders name. */
    private static final String NODEIDX_UID_SEPARATOR = "-";

    /** Constant node subfolder prefix and node index pattern (nodeII, where II - node index as decimal integer) */
    private static final String NODE_PATTERN = DB_FOLDER_PREFIX + "[0-9]*" + NODEIDX_UID_SEPARATOR;

    /** Uuid as string pattern. */
    private static final String UUID_STR_PATTERN = "[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}";

    /**
     * Subdir (nodeII-UID, where II - node index as decimal integer, UID - string representation of consistent ID)
     * pattern.
     */
    private static final String SUBDIR_PATTERN = NODE_PATTERN + UUID_STR_PATTERN;

    /** Database subfolders for new style filter. */
    public static final FileFilter DB_SUBFOLDERS_NEW_STYLE_FILTER = new FileFilter() {
        @Override public boolean accept(File pathname) {
            return pathname.isDirectory() && pathname.getName().matches(SUBDIR_PATTERN);
        }
    };

    /** Database subfolders for old style filter. */
    private static final FileFilter DB_SUBFOLDERS_OLD_STYLE_FILTER = new FileFilter() {
        @Override public boolean accept(File pathname) {
            return pathname.isDirectory()
                && !"wal".equals(pathname.getName())
                && !pathname.getName().matches(SUBDIR_PATTERN);
        }
    };

    /** Database default folder. */
    public static final String DB_DEFAULT_FOLDER = "db";

    /** Config. */
    private IgniteConfiguration cfg;

    /** Logger. */
    private IgniteLogger log;

    /** Context. */
    private GridKernalContext ctx;

    /** Cached folder settings. */
    private PdsFolderSettings settings;

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
    private PdsFolderSettings compatibleResolve(
        @Nullable final File pstStoreBasePath,
        @NotNull final Serializable consistentId) {

        if (cfg.getConsistentId() != null) {
            // compatible mode from configuration is used fot this case, no locking, no consitent id change
            return new PdsFolderSettings(pstStoreBasePath, cfg.getConsistentId());
        }

        return new PdsFolderSettings(pstStoreBasePath, consistentId);
    }

    /** {@inheritDoc} */
    @Override public PdsFolderSettings resolveFolders() throws IgniteCheckedException {
        if (settings == null) {
            settings = prepareNewSettings();

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
     * Creates new settings when we don't have cached one.
     *
     * @return new settings with prelocked directory (if appropriate).
     * @throws IgniteCheckedException if IO failed.
     */
    private PdsFolderSettings prepareNewSettings() throws IgniteCheckedException {
        final File pstStoreBasePath = resolvePersistentStoreBasePath();
        //here deprecated method is used to get compatible version of consistentId
        final Serializable consistentId = ctx.discovery().consistentId();

        if (!CU.isPersistenceEnabled(cfg))
            return compatibleResolve(pstStoreBasePath, consistentId);

        if (ctx.clientNode())
            return new PdsFolderSettings(pstStoreBasePath, UUID.randomUUID());

        if (getBoolean(IGNITE_DATA_STORAGE_FOLDER_BY_CONSISTENT_ID, false))
            return compatibleResolve(pstStoreBasePath, consistentId);

        // compatible mode from configuration is used fot this case
        if (cfg.getConsistentId() != null) {
            // compatible mode from configuration is used fot this case, no locking, no consistent id change
            return new PdsFolderSettings(pstStoreBasePath, cfg.getConsistentId());
        }
        // The node scans the work directory and checks if there is a folder matching the consistent ID.
        // If such a folder exists, we start up with this ID (compatibility mode)
        final String subFolder = U.maskForFileName(consistentId.toString());

        final GridCacheDatabaseSharedManager.FileLockHolder oldStyleFolderLockHolder = tryLock(new File(pstStoreBasePath, subFolder));

        if (oldStyleFolderLockHolder != null)
            return new PdsFolderSettings(pstStoreBasePath,
                subFolder,
                consistentId,
                oldStyleFolderLockHolder,
                true);

        final File[] oldStyleFolders = pstStoreBasePath.listFiles(DB_SUBFOLDERS_OLD_STYLE_FILTER);

        if (oldStyleFolders != null && oldStyleFolders.length != 0) {
            for (File folder : oldStyleFolders) {
                final String path = getPathDisplayableInfo(folder);

                U.warn(log, "There is other non-empty storage folder under storage base directory [" + path + "]");
            }
        }

        for (FolderCandidate next : getNodeIndexSortedCandidates(pstStoreBasePath)) {
            final GridCacheDatabaseSharedManager.FileLockHolder fileLockHolder = tryLock(next.subFolderFile());

            if (fileLockHolder != null) {
                if (log.isInfoEnabled())
                    log.info("Successfully locked persistence storage folder [" + next.subFolderFile() + "]");

                return new PdsFolderSettings(pstStoreBasePath,
                    next.subFolderFile().getName(),
                    next.uuid(),
                    fileLockHolder,
                    false);
            }
        }

        // was not able to find free slot, allocating new
        try (final GridCacheDatabaseSharedManager.FileLockHolder rootDirLock = lockRootDirectory(pstStoreBasePath)) {
            final List<FolderCandidate> sortedCandidates = getNodeIndexSortedCandidates(pstStoreBasePath);
            final int nodeIdx = sortedCandidates.isEmpty() ? 0 : (sortedCandidates.get(sortedCandidates.size() - 1).nodeIndex() + 1);

            return generateAndLockNewDbStorage(pstStoreBasePath, nodeIdx);
        }
    }

    /**
     * Calculate overall folder size.
     *
     * @param dir directory to scan.
     * @return total size in bytes.
     */
    private static FolderParams folderSize(File dir) {
        final FolderParams params = new FolderParams();

        visitFolder(dir, params);

        return params;
    }

    /**
     * Scans provided directory and its sub dirs, collects found metrics.
     *
     * @param dir directory to start scan from.
     * @param params input/output.
     */
    private static void visitFolder(final File dir, final FolderParams params) {
        for (File file : dir.listFiles()) {
            if (file.isDirectory())
                visitFolder(file, params);
            else {
                params.size += file.length();
                params.lastModified = Math.max(params.lastModified, dir.lastModified());
            }
        }
    }

    /**
     * @param folder folder to scan.
     * @return folder displayable information.
     */
    @NotNull private String getPathDisplayableInfo(final File folder) {
        final SB res = new SB();

        res.a(getCanonicalPath(folder));
        res.a(", ");
        final FolderParams params = folderSize(folder);

        res.a(params.size);
        res.a(" bytes, modified ");
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm a");

        res.a(simpleDateFormat.format(params.lastModified));
        res.a(" ");

        return res.toString();
    }

    /**
     * Returns the canonical pathname string of this abstract pathname.
     *
     * @param file path to convert.
     * @return canonical pathname or at leas absolute if convert to canonical failed.
     */
    @NotNull private String getCanonicalPath(final File file) {
        try {
            return file.getCanonicalPath();
        }
        catch (IOException ignored) {
            return file.getAbsolutePath();
        }
    }

    /**
     * Pad start of string with provided character.
     *
     * @param str sting to pad.
     * @param minLength expected length.
     * @param padChar padding character.
     * @return padded string.
     */
    private static String padStart(String str, int minLength, char padChar) {
        A.notNull(str, "String should not be empty");
        if (str.length() >= minLength)
            return str;

        final SB sb = new SB(minLength);

        for (int i = str.length(); i < minLength; ++i)
            sb.a(padChar);

        sb.a(str);

        return sb.toString();

    }

    /**
     * Creates new DB storage folder.
     *
     * @param pstStoreBasePath DB root path.
     * @param nodeIdx next node index to use in folder name.
     * @return new settings to be used in this node.
     * @throws IgniteCheckedException if failed.
     */
    @NotNull private PdsFolderSettings generateAndLockNewDbStorage(final File pstStoreBasePath,
        final int nodeIdx) throws IgniteCheckedException {

        final UUID uuid = UUID.randomUUID();
        final String consIdBasedFolder = genNewStyleSubfolderName(nodeIdx, uuid);
        final File newRandomFolder = U.resolveWorkDirectory(pstStoreBasePath.getAbsolutePath(), consIdBasedFolder, false); //mkdir here
        final GridCacheDatabaseSharedManager.FileLockHolder fileLockHolder = tryLock(newRandomFolder);

        if (fileLockHolder != null) {
            if (log.isInfoEnabled())
                log.info("Successfully created new persistent storage folder [" + newRandomFolder + "]");

            return new PdsFolderSettings(pstStoreBasePath, consIdBasedFolder, uuid, fileLockHolder, false);
        }
        throw new IgniteCheckedException("Unable to lock file generated randomly [" + newRandomFolder + "]");
    }

    /**
     * Generates DB subfolder name for provided node index (local) and UUID (consistent ID)
     *
     * @param nodeIdx node index.
     * @param uuid consistent ID.
     * @return folder file name
     */
    @NotNull public static String genNewStyleSubfolderName(final int nodeIdx, final UUID uuid) {
        final String uuidAsStr = uuid.toString();

        assert uuidAsStr.matches(UUID_STR_PATTERN);

        final String nodeIdxPadded = padStart(Integer.toString(nodeIdx), 2, '0');

        return DB_FOLDER_PREFIX + nodeIdxPadded + NODEIDX_UID_SEPARATOR + uuidAsStr;
    }

    /**
     * Acquires lock to root storage directory, used to lock root directory in case creating new files is required.
     *
     * @param pstStoreBasePath rood DB dir to lock
     * @return locked directory, should be released and closed later
     * @throws IgniteCheckedException if failed
     */
    @NotNull private GridCacheDatabaseSharedManager.FileLockHolder lockRootDirectory(File pstStoreBasePath)
        throws IgniteCheckedException {

        GridCacheDatabaseSharedManager.FileLockHolder rootDirLock;
        int retry = 0;

        while ((rootDirLock = tryLock(pstStoreBasePath)) == null) {
            if (retry > 600)
                throw new IgniteCheckedException("Unable to start under DB storage path [" + pstStoreBasePath + "]" +
                    ". Lock is being held to root directory");
            retry++;
        }

        return rootDirLock;
    }

    /**
     * @param pstStoreBasePath root storage folder to scan.
     * @return empty list if there is no files in folder to test. Non null value is returned for folder having
     * applicable new style files. Collection is sorted ascending according to node ID, 0 node index is coming first.
     */
    @Nullable private List<FolderCandidate> getNodeIndexSortedCandidates(File pstStoreBasePath) {
        final File[] files = pstStoreBasePath.listFiles(DB_SUBFOLDERS_NEW_STYLE_FILTER);

        if (files == null)
            return Collections.emptyList();

        final List<FolderCandidate> res = new ArrayList<>();

        for (File file : files) {
            final FolderCandidate candidate = parseFileName(file);

            if (candidate != null)
                res.add(candidate);
        }
        Collections.sort(res, new Comparator<FolderCandidate>() {
            @Override public int compare(FolderCandidate c1, FolderCandidate c2) {
                return Integer.compare(c1.nodeIndex(), c2.nodeIndex());
            }
        });

        return res;
    }

    /**
     * Tries to lock subfolder within storage root folder.
     *
     * @param dbStoreDirWithSubdirectory DB store directory, is to be absolute and should include consistent ID based
     * sub folder.
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

            if (log.isInfoEnabled())
                log.info("Unable to acquire lock to file [" + path + "], reason: " + e.getMessage());

            return null;
        }
    }

    /**
     * @return DB storage absolute root path resolved as 'db' folder in Ignite work dir (by default) or using persistent
     * store configuration. Null if persistence is not enabled. Returned folder is created automatically.
     * @throws IgniteCheckedException if I/O failed.
     */
    @Nullable private File resolvePersistentStoreBasePath() throws IgniteCheckedException {
        final DataStorageConfiguration dsCfg = cfg.getDataStorageConfiguration();

        if (dsCfg == null)
            return null;

        final String pstPath = dsCfg.getStoragePath();

        return U.resolveWorkDirectory(
            cfg.getWorkDirectory(),
            pstPath != null ? pstPath : DB_DEFAULT_FOLDER,
            false
        );

    }

    /**
     * @param subFolderFile new style folder name to parse
     * @return Pair of UUID and node index
     */
    private FolderCandidate parseFileName(@NotNull final File subFolderFile) {
        return parseSubFolderName(subFolderFile, log);
    }

    /**
     * @param subFolderFile new style file to parse.
     * @param log Logger.
     * @return Pair of UUID and node index.
     */
    @Nullable public static FolderCandidate parseSubFolderName(
        @NotNull final File subFolderFile,
        @NotNull final IgniteLogger log) {

        final String fileName = subFolderFile.getName();
        final Matcher matcher = Pattern.compile(NODE_PATTERN).matcher(fileName);
        if (!matcher.find())
            return null;

        int uidStart = matcher.end();

        try {
            final String uid = fileName.substring(uidStart);
            final UUID uuid = UUID.fromString(uid);
            final String substring = fileName.substring(DB_FOLDER_PREFIX.length(), uidStart - NODEIDX_UID_SEPARATOR.length());
            final int idx = Integer.parseInt(substring);

            return new FolderCandidate(subFolderFile, idx, uuid);
        }
        catch (Exception e) {
            U.warn(log, "Unable to parse new style file format from [" + subFolderFile.getAbsolutePath() + "]: " + e);

            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        if (settings != null) {
            final GridCacheDatabaseSharedManager.FileLockHolder fileLockHolder = settings.getLockedFileLockHolder();

            if (fileLockHolder != null)
                fileLockHolder.close();
        }

        super.stop(cancel);
    }

    /** Path metrics */
    private static class FolderParams {
        /** Overall size in bytes. */
        private long size;

        /** Last modified. */
        private long lastModified;
    }

    /**
     * Represents parsed new style file and encoded parameters in this file name
     */
    public static class FolderCandidate {
        /** Absolute file path pointing to DB subfolder within DB storage root folder. */
        private final File subFolderFile;

        /** Node index (local, usually 0 if multiple nodes are not started at local PC). */
        private final int nodeIdx;

        /** Uuid contained in file name, is to be set as consistent ID. */
        private final UUID uuid;

        /**
         * @param subFolderFile Absolute file path pointing to DB subfolder.
         * @param nodeIdx Node index.
         * @param uuid Uuid.
         */
        public FolderCandidate(File subFolderFile, int nodeIdx, UUID uuid) {
            this.subFolderFile = subFolderFile;
            this.nodeIdx = nodeIdx;
            this.uuid = uuid;
        }

        /**
         * @return Node index (local, usually 0 if multiple nodes are not started at local PC).
         */
        public int nodeIndex() {
            return nodeIdx;
        }

        /**
         * @return Uuid contained in file name, is to be set as consistent ID.
         */
        public Serializable uuid() {
            return uuid;
        }

        /**
         * @return Absolute file path pointing to DB subfolder within DB storage root folder.
         */
        public File subFolderFile() {
            return subFolderFile;
        }
    }
}


