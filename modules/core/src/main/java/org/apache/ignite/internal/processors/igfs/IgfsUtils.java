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

package org.apache.ignite.internal.processors.igfs;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.IgfsEvent;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.lang.IgniteOutClosureX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CACHE_RETRIES_COUNT;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.igfs.IgfsMode.DUAL_ASYNC;
import static org.apache.ignite.igfs.IgfsMode.DUAL_SYNC;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGFS;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Common IGFS utility methods.
 */
public class IgfsUtils {
    /** ID for the root directory. */
    public static final IgniteUuid ROOT_ID = new IgniteUuid(new UUID(0, 0), 0);

    /** Lock Id used to lock files being deleted from TRASH. This is a global constant. */
    public static final IgniteUuid DELETE_LOCK_ID = new IgniteUuid(new UUID(0, 0), 0);

    /** Constant trash concurrency level. */
    public static final int TRASH_CONCURRENCY = 64;

    /** File property: user name. */
    public static final String PROP_USER_NAME = "usrName";

    /** File property: group name. */
    public static final String PROP_GROUP_NAME = "grpName";

    /** File property: permission. */
    public static final String PROP_PERMISSION = "permission";

    /** File property: prefer writes to local node. */
    public static final String PROP_PREFER_LOCAL_WRITES = "locWrite";

    /** Generic property index. */
    private static final byte PROP_IDX = 0;

    /** User name property index. */
    private static final byte PROP_USER_NAME_IDX = 1;

    /** Group name property index. */
    private static final byte PROP_GROUP_NAME_IDX = 2;

    /** Permission property index. */
    private static final byte PROP_PERMISSION_IDX = 3;

    /** Prefer local writes property index. */
    private static final byte PROP_PREFER_LOCAL_WRITES_IDX = 4;

    /** Trash directory IDs. */
    private static final IgniteUuid[] TRASH_IDS;

    /** Maximum number of file unlock transaction retries when topology changes. */
    private static final int MAX_CACHE_TX_RETRIES = IgniteSystemProperties.getInteger(IGNITE_CACHE_RETRIES_COUNT, 100);

    /** Separator between id and name parts in the trash name. */
    private static final char TRASH_NAME_SEPARATOR = '|';

    /** Flag: this is a directory. */
    private static final byte FLAG_DIR = 0x1;

    /** Flag: this is a file. */
    private static final byte FLAG_FILE = 0x2;

    /** Filesystem cache prefix. */
    public static final String IGFS_CACHE_PREFIX = "igfs-internal-";

    /** Data cache suffix. */
    public static final String DATA_CACHE_SUFFIX = "-data";

    /** Meta cache suffix. */
    public static final String META_CACHE_SUFFIX = "-meta";

    /** Maximum string length to be written at once. */
    private static final int MAX_STR_LEN = 0xFFFF / 4;

    /** Min available TCP port. */
    private static final int MIN_TCP_PORT = 1;

    /** Max available TCP port. */
    private static final int MAX_TCP_PORT = 0xFFFF;

    /*
     * Static initializer.
     */
    static {
        TRASH_IDS = new IgniteUuid[TRASH_CONCURRENCY];

        for (int i = 0; i < TRASH_CONCURRENCY; i++)
            TRASH_IDS[i] = new IgniteUuid(new UUID(0, i + 1), 0);
    }

    /**
     * Get random trash ID.
     *
     * @return Trash ID.
     */
    public static IgniteUuid randomTrashId() {
        return TRASH_IDS[ThreadLocalRandom.current().nextInt(TRASH_CONCURRENCY)];
    }

    /**
     * Get trash ID for the given index.
     *
     * @param idx Index.
     * @return Trahs ID.
     */
    public static IgniteUuid trashId(int idx) {
        assert idx >= 0 && idx < TRASH_CONCURRENCY;

        return TRASH_IDS[idx];
    }

    /**
     * Check whether provided ID is either root ID or trash ID.
     *
     * @param id ID.
     * @return {@code True} if this is root ID or trash ID.
     */
    public static boolean isRootOrTrashId(@Nullable IgniteUuid id) {
        return isRootId(id) || isTrashId(id);
    }

    /**
     * Check whether provided ID is root ID.
     *
     * @param id ID.
     * @return {@code True} if this is root ID.
     */
    public static boolean isRootId(@Nullable IgniteUuid id) {
        return id != null && ROOT_ID.equals(id);
    }

    /**
     * Check whether provided ID is trash ID.
     *
     * @param id ID.
     * @return {@code True} if this is trash ID.
     */
    private static boolean isTrashId(IgniteUuid id) {
        if (id == null)
            return false;

        UUID gid = id.globalId();

        return id.localId() == 0 && gid.getMostSignificantBits() == 0 &&
            gid.getLeastSignificantBits() > 0 && gid.getLeastSignificantBits() <= TRASH_CONCURRENCY;
    }

    /**
     * Converts any passed exception to IGFS exception.
     *
     * @param err Initial exception.
     * @return Converted IGFS exception.
     */
    public static IgfsException toIgfsException(Throwable err) {
        IgfsException err0 = err instanceof IgfsException ? (IgfsException)err : null;

        IgfsException igfsErr = X.cause(err, IgfsException.class);

        while (igfsErr != null && igfsErr != err0) {
            err0 = igfsErr;

            igfsErr = X.cause(err, IgfsException.class);
        }

        // If initial exception is already IGFS exception and no inner stuff exists, just return it unchanged.
        if (err0 != err) {
            if (err0 != null)
                // Dealing with a kind of IGFS error, wrap it once again, preserving message and root cause.
                err0 = newIgfsException(err0.getClass(), err0.getMessage(), err0);
            else {
                if (err instanceof ClusterTopologyServerNotFoundException)
                    err0 = new IgfsException("Cache server nodes not found.", err);
                else
                    // Unknown error nature.
                    err0 = new IgfsException("Generic IGFS error occurred.", err);
            }
        }

        return err0;
    }

    /**
     * Construct new IGFS exception passing specified message and cause.
     *
     * @param cls Class.
     * @param msg Message.
     * @param cause Cause.
     * @return New IGFS exception.
     */
    public static IgfsException newIgfsException(Class<? extends IgfsException> cls, String msg, Throwable cause) {
        try {
            Constructor<? extends IgfsException> ctor = cls.getConstructor(String.class, Throwable.class);

            return ctor.newInstance(msg, cause);
        }
        catch (ReflectiveOperationException e) {
            throw new IgniteException("Failed to create IGFS exception: " + cls.getName(), e);
        }
    }

    /**
     * Constructor.
     */
    private IgfsUtils() {
        // No-op.
    }

    /**
     * Provides non-null user name.
     * If the user name is null or empty string, defaults to {@link FileSystemConfiguration#DFLT_USER_NAME},
     * which is the current process owner user.
     * @param user a user name to be fixed.
     * @return non-null interned user name.
     */
    public static String fixUserName(@Nullable String user) {
        if (F.isEmpty(user))
           user = FileSystemConfiguration.DFLT_USER_NAME;

        return user;
    }

    /**
     * Performs an operation with transaction with retries.
     *
     * @param cache Cache to do the transaction on.
     * @param clo Closure.
     * @return Result of closure execution.
     * @throws IgniteCheckedException If failed.
     */
    public static <T> T doInTransactionWithRetries(IgniteInternalCache cache, IgniteOutClosureX<T> clo)
        throws IgniteCheckedException {
        assert cache != null;

        int attempts = 0;

        while (attempts < MAX_CACHE_TX_RETRIES) {
            try (Transaction tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                T res = clo.applyx();

                tx.commit();

                return res;
            }
            catch (IgniteException | IgniteCheckedException e) {
                ClusterTopologyException cte = X.cause(e, ClusterTopologyException.class);

                if (cte != null)
                    ((IgniteFutureImpl)cte.retryReadyFuture()).internalFuture().getUninterruptibly();
                else
                    throw U.cast(e);
            }

            attempts++;
        }

        throw new IgniteCheckedException("Failed to perform operation since max number of attempts " +
            "exceeded. [maxAttempts=" + MAX_CACHE_TX_RETRIES + ']');
    }

    /**
     * Sends a series of event.
     *
     * @param kernalCtx Kernal context.
     * @param path The path of the created file.
     * @param type The type of event to send.
     */
    public static void sendEvents(GridKernalContext kernalCtx, IgfsPath path, int type) {
        sendEvents(kernalCtx, path, null, type);
    }

    /**
     * Sends a series of event.
     *
     * @param kernalCtx Kernal context.
     * @param path The path of the created file.
     * @param newPath New path.
     * @param type The type of event to send.
     */
    public static void sendEvents(GridKernalContext kernalCtx, IgfsPath path, IgfsPath newPath, int type) {
        assert kernalCtx != null;
        assert path != null;

        GridEventStorageManager evts = kernalCtx.event();

        ClusterNode locNode = kernalCtx.discovery().localNode();

        if (evts.isRecordable(type)) {
            if (newPath == null)
                evts.record(new IgfsEvent(path, locNode, type));
            else
                evts.record(new IgfsEvent(path, newPath, locNode, type));
        }
    }

    /**
     * @param cacheName Cache name.
     * @return {@code True} in this is IGFS data or meta cache.
     */
    public static boolean matchIgfsCacheName(@Nullable String cacheName) {
        return cacheName != null && cacheName.startsWith(IGFS_CACHE_PREFIX);
    }

    /**
     * @param cfg Grid configuration.
     * @param cacheName Cache name.
     * @return {@code True} in this is IGFS data or meta cache.
     */
    public static boolean isIgfsCache(IgniteConfiguration cfg, @Nullable String cacheName) {
        return matchIgfsCacheName(cacheName);
    }

    /**
     * Prepare cache configuration if this is IGFS meta or data cache.
     *
     * @param cfg Configuration.
     * @throws IgniteCheckedException If failed.
     */
    public static void prepareCacheConfigurations(IgniteConfiguration cfg) throws IgniteCheckedException {
        FileSystemConfiguration[] igfsCfgs = cfg.getFileSystemConfiguration();
        List<CacheConfiguration> ccfgs = new ArrayList<>(Arrays.asList(cfg.getCacheConfiguration()));

        if (igfsCfgs != null) {
            for (FileSystemConfiguration igfsCfg : igfsCfgs) {
                if (igfsCfg == null)
                    continue;

                CacheConfiguration ccfgMeta = igfsCfg.getMetaCacheConfiguration();

                if (ccfgMeta == null) {
                    ccfgMeta = defaultMetaCacheConfig();

                    igfsCfg.setMetaCacheConfiguration(ccfgMeta);
                }

                ccfgMeta.setName(IGFS_CACHE_PREFIX + igfsCfg.getName() + META_CACHE_SUFFIX);

                ccfgs.add(ccfgMeta);

                CacheConfiguration ccfgData = igfsCfg.getDataCacheConfiguration();

                if (ccfgData == null) {
                    ccfgData = defaultDataCacheConfig();

                    igfsCfg.setDataCacheConfiguration(ccfgData);
                }

                ccfgData.setName(IGFS_CACHE_PREFIX + igfsCfg.getName() + DATA_CACHE_SUFFIX);

                ccfgs.add(ccfgData);

                // No copy-on-read.
                ccfgMeta.setCopyOnRead(false);
                ccfgData.setCopyOnRead(false);

                // Always full-sync to maintain consistency.
                ccfgMeta.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
                ccfgData.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

                // Set co-located affinity mapper if needed.
                if (igfsCfg.isColocateMetadata() && ccfgMeta.getAffinityMapper() == null)
                    ccfgMeta.setAffinityMapper(new IgfsColocatedMetadataAffinityKeyMapper());

                // Set affinity mapper if needed.
                if (ccfgData.getAffinityMapper() == null)
                    ccfgData.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper());
            }

            cfg.setCacheConfiguration(ccfgs.toArray(new CacheConfiguration[ccfgs.size()]));
        }

        validateLocalIgfsConfigurations(cfg);
    }

    /**
     * Validates local IGFS configurations. Compares attributes only for IGFSes with same name.
     *
     * @param igniteCfg Ignite config.
     * @throws IgniteCheckedException If any of IGFS configurations is invalid.
     */
    private static void validateLocalIgfsConfigurations(IgniteConfiguration igniteCfg)
        throws IgniteCheckedException {

        if (igniteCfg.getFileSystemConfiguration() == null || igniteCfg.getFileSystemConfiguration().length == 0)
            return;

        Collection<String> cfgNames = new HashSet<>();

        for (FileSystemConfiguration cfg : igniteCfg.getFileSystemConfiguration()) {
            String name = cfg.getName();

            if (name == null)
                throw new IgniteCheckedException("IGFS name cannot be null");

            if (cfgNames.contains(name))
                throw new IgniteCheckedException("Duplicate IGFS name found (check configuration and " +
                    "assign unique name to each): " + name);

            CacheConfiguration ccfgData = cfg.getDataCacheConfiguration();

            CacheConfiguration ccfgMeta = cfg.getMetaCacheConfiguration();

            if (QueryUtils.isEnabled(ccfgData))
                throw new IgniteCheckedException("IGFS data cache cannot start with enabled query indexing.");

            if (QueryUtils.isEnabled(ccfgMeta))
                throw new IgniteCheckedException("IGFS metadata cache cannot start with enabled query indexing.");

            if (ccfgMeta.getAtomicityMode() != TRANSACTIONAL)
                throw new IgniteCheckedException("IGFS metadata cache should be transactional: " + cfg.getName());

            if (!(ccfgData.getAffinityMapper() instanceof IgfsGroupDataBlocksKeyMapper))
                throw new IgniteCheckedException(
                    "Invalid IGFS data cache configuration (key affinity mapper class should be " +
                    IgfsGroupDataBlocksKeyMapper.class.getSimpleName() + "): " + cfg);

            IgfsIpcEndpointConfiguration ipcCfg = cfg.getIpcEndpointConfiguration();

            if (ipcCfg != null) {
                final int tcpPort = ipcCfg.getPort();

                if (!(tcpPort >= MIN_TCP_PORT && tcpPort <= MAX_TCP_PORT))
                    throw new IgniteCheckedException("IGFS endpoint TCP port is out of range [" + MIN_TCP_PORT +
                        ".." + MAX_TCP_PORT + "]: " + tcpPort);

                if (ipcCfg.getThreadCount() <= 0)
                    throw new IgniteCheckedException("IGFS endpoint thread count must be positive: " +
                        ipcCfg.getThreadCount());
            }

            boolean secondary = cfg.getDefaultMode() == IgfsMode.PROXY;

            if (cfg.getPathModes() != null) {
                for (Map.Entry<String, IgfsMode> mode : cfg.getPathModes().entrySet()) {
                    if (mode.getValue() == IgfsMode.PROXY)
                        secondary = true;
                }
            }

            if (secondary && cfg.getSecondaryFileSystem() == null) {
                // When working in any mode except of primary, secondary FS config must be provided.
                throw new IgniteCheckedException("Grid configuration parameter invalid: " +
                    "secondaryFileSystem cannot be null when mode is not " + IgfsMode.PRIMARY);
            }

            cfgNames.add(name);
        }
    }

    /**
     * @return Default IGFS cache configuration.
     */
    private static CacheConfiguration defaultCacheConfig() {
        CacheConfiguration cfg = new CacheConfiguration();
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setCacheMode(CacheMode.PARTITIONED);

        return cfg;
    }

    /**
     * @return Default IGFS meta cache configuration.
     */
    private static CacheConfiguration defaultMetaCacheConfig() {
        CacheConfiguration cfg = defaultCacheConfig();

        cfg.setBackups(1);

        return cfg;
    }

    /**
     * @return Default IGFS data cache configuration.
     */
    private static CacheConfiguration defaultDataCacheConfig() {
        return defaultCacheConfig();
    }

    /**
     * Create empty directory with the given ID.
     *
     * @param id ID.
     * @return File info.
     */
    public static IgfsDirectoryInfo createDirectory(IgniteUuid id) {
        return createDirectory(id, null, null);
    }

    /**
     * Create directory.
     *
     * @param id ID.
     * @param listing Listing.
     * @param props Properties.
     * @return File info.
     */
    public static IgfsDirectoryInfo createDirectory(
        IgniteUuid id,
        @Nullable Map<String, IgfsListingEntry> listing,
        @Nullable Map<String, String> props) {
        long time = System.currentTimeMillis();

        return createDirectory(id, listing, props, time, time);
    }

    /**
     * Create directory.
     *
     * @param id ID.
     * @param listing Listing.
     * @param props Properties.
     * @param createTime Create time.
     * @param modificationTime Modification time.
     * @return File info.
     */
    public static IgfsDirectoryInfo createDirectory(
        IgniteUuid id,
        @Nullable Map<String, IgfsListingEntry> listing,
        @Nullable Map<String,String> props,
        long createTime,
        long modificationTime) {
        return new IgfsDirectoryInfo(id, listing, props, createTime, modificationTime);
    }

    /**
     * Create file.
     *
     * @param id File ID.
     * @param blockSize Block size.
     * @param len Length.
     * @param affKey Affinity key.
     * @param lockId Lock ID.
     * @param evictExclude Evict exclude flag.
     * @param props Properties.
     * @param accessTime Access time.
     * @param modificationTime Modification time.
     * @return File info.
     */
    public static IgfsFileInfo createFile(IgniteUuid id, int blockSize, long len, @Nullable IgniteUuid affKey,
        @Nullable IgniteUuid lockId, boolean evictExclude, @Nullable Map<String, String> props, long accessTime,
        long modificationTime) {
        return new IgfsFileInfo(id, blockSize, len, affKey, props, null, lockId, accessTime, modificationTime,
            evictExclude);
    }

    /**
     * Write listing entry.
     *
     * @param out Writer.
     * @param entry Entry.
     */
    public static void writeListingEntry(BinaryRawWriter out, @Nullable IgfsListingEntry entry) {
        if (entry != null) {
            out.writeBoolean(true);

            BinaryUtils.writeIgniteUuid(out, entry.fileId());

            out.writeBoolean(entry.isDirectory());
        }
        else
            out.writeBoolean(false);
    }

    /**
     * Read listing entry.
     *
     * @param in Reader.
     * @return Entry.
     */
    @Nullable public static IgfsListingEntry readListingEntry(BinaryRawReader in) {
        if (in.readBoolean()) {
            IgniteUuid id = BinaryUtils.readIgniteUuid(in);
            boolean dir = in.readBoolean();

            return new IgfsListingEntry(id, dir);
        }
        else
            return null;
    }

    /**
     * Write listing entry.
     *
     * @param out Writer.
     * @param entry Entry.
     * @throws IOException If failed.
     */
    public static void writeListingEntry(DataOutput out, @Nullable IgfsListingEntry entry) throws IOException {
        if (entry != null) {
            out.writeBoolean(true);

            IgniteUtils.writeGridUuid(out, entry.fileId());

            out.writeBoolean(entry.isDirectory());
        }
        else
            out.writeBoolean(false);
    }

    /**
     * Read listing entry.
     *
     * @param in Reader.
     * @return Entry.
     * @throws IOException If failed.
     */
    @Nullable public static IgfsListingEntry readListingEntry(DataInput in) throws IOException {
        if (in.readBoolean()) {
            IgniteUuid id = IgniteUtils.readGridUuid(in);
            boolean dir = in.readBoolean();

            return new IgfsListingEntry(id, dir);
        }
        else
            return null;
    }

    /**
     * Write entry properties. Rely on reference equality for well-known properties.
     *
     * @param out Writer.
     * @param props Properties.
     */
    @SuppressWarnings("StringEquality")
    public static void writeProperties(BinaryRawWriter out, @Nullable Map<String, String> props) {
        if (props != null) {
            out.writeInt(props.size());

            for (Map.Entry<String, String> entry : props.entrySet()) {
                String key = entry.getKey();

                if (key == PROP_PERMISSION)
                    out.writeByte(PROP_PERMISSION_IDX);
                else if (key == PROP_PREFER_LOCAL_WRITES)
                    out.writeByte(PROP_PREFER_LOCAL_WRITES_IDX);
                else if (key == PROP_USER_NAME)
                    out.writeByte(PROP_USER_NAME_IDX);
                else if (key == PROP_GROUP_NAME)
                    out.writeByte(PROP_GROUP_NAME_IDX);
                else {
                    out.writeByte(PROP_IDX);
                    out.writeString(key);
                }

                out.writeString(entry.getValue());
            }
        }
        else
            out.writeInt(-1);
    }

    /**
     * Read entry properties.
     *
     * @param in Reader.
     * @return Properties.
     */
    @Nullable public static Map<String, String> readProperties(BinaryRawReader in) {
        int size = in.readInt();

        if (size >= 0) {
            Map<String, String> props = new HashMap<>(size);

            for (int i = 0; i < size; i++) {
                byte idx = in.readByte();

                String key;

                switch (idx) {
                    case PROP_PERMISSION_IDX:
                        key = PROP_PERMISSION;

                        break;

                    case PROP_PREFER_LOCAL_WRITES_IDX:
                        key = PROP_PREFER_LOCAL_WRITES;

                        break;

                    case PROP_USER_NAME_IDX:
                        key = PROP_USER_NAME;

                        break;

                    case PROP_GROUP_NAME_IDX:
                        key = PROP_GROUP_NAME;

                        break;

                    default:
                        key = in.readString();
                }

                props.put(key, in.readString());
            }

            return props;
        }
        else
            return null;
    }

    /**
     * Write entry properties. Rely on reference equality for well-known properties.
     *
     * @param out Writer.
     * @param props Properties.
     * @throws IOException If failed.
     */
    @SuppressWarnings("StringEquality")
    public static void writeProperties(DataOutput out, @Nullable Map<String, String> props) throws IOException {
        if (props != null) {
            out.writeInt(props.size());

            for (Map.Entry<String, String> entry : props.entrySet()) {
                String key = entry.getKey();

                if (key == PROP_PERMISSION)
                    out.writeByte(PROP_PERMISSION_IDX);
                else if (key == PROP_PREFER_LOCAL_WRITES)
                    out.writeByte(PROP_PREFER_LOCAL_WRITES_IDX);
                else if (key == PROP_USER_NAME)
                    out.writeByte(PROP_USER_NAME_IDX);
                else if (key == PROP_GROUP_NAME)
                    out.writeByte(PROP_GROUP_NAME_IDX);
                else {
                    out.writeByte(PROP_IDX);
                    U.writeString(out, key);
                }

                U.writeString(out, entry.getValue());
            }
        }
        else
            out.writeInt(-1);
    }

    /**
     * Read entry properties.
     *
     * @param in Reader.
     * @return Properties.
     * @throws IOException If failed.
     */
    @Nullable public static Map<String, String> readProperties(DataInput in) throws IOException {
        int size = in.readInt();

        if (size >= 0) {
            Map<String, String> props = new HashMap<>(size);

            for (int i = 0; i < size; i++) {
                byte idx = in.readByte();

                String key;

                switch (idx) {
                    case PROP_PERMISSION_IDX:
                        key = PROP_PERMISSION;

                        break;

                    case PROP_PREFER_LOCAL_WRITES_IDX:
                        key = PROP_PREFER_LOCAL_WRITES;

                        break;

                    case PROP_USER_NAME_IDX:
                        key = PROP_USER_NAME;

                        break;

                    case PROP_GROUP_NAME_IDX:
                        key = PROP_GROUP_NAME;

                        break;

                    default:
                        key = U.readString(in);
                }

                props.put(key, U.readString(in));
            }

            return props;
        }
        else
            return null;
    }

    /**
     * Write IGFS path.
     *
     * @param writer Writer.
     * @param path Path.
     */
    public static void writePath(BinaryRawWriter writer, @Nullable IgfsPath path) {
        if (path != null) {
            writer.writeBoolean(true);

            path.writeRawBinary(writer);
        }
        else
            writer.writeBoolean(false);
    }

    /**
     * Read IGFS path.
     *
     * @param reader Reader.
     * @return Path.
     */
    @Nullable public static IgfsPath readPath(BinaryRawReader reader) {
        if (reader.readBoolean()) {
            IgfsPath path = new IgfsPath();

            path.readRawBinary(reader);

            return path;
        }
        else
            return null;
    }

    /**
     * Read non-null path from the input.
     *
     * @param in Input.
     * @return IGFS path.
     * @throws IOException If failed.
     */
    public static IgfsPath readPath(ObjectInput in) throws IOException {
        IgfsPath res = new IgfsPath();

        res.readExternal(in);

        return res;
    }

    /**
     * Write IgfsFileAffinityRange.
     *
     * @param writer Writer
     * @param affRange affinity range.
     */
    public static void writeFileAffinityRange(BinaryRawWriter writer, @Nullable IgfsFileAffinityRange affRange) {
        if (affRange != null) {
            writer.writeBoolean(true);

            affRange.writeRawBinary(writer);
        }
        else
            writer.writeBoolean(false);
    }

    /**
     * Read IgfsFileAffinityRange.
     *
     * @param reader Reader.
     * @return File affinity range.
     */
    public static IgfsFileAffinityRange readFileAffinityRange(BinaryRawReader reader) {
        if (reader.readBoolean()) {
            IgfsFileAffinityRange affRange = new IgfsFileAffinityRange();

            affRange.readRawBinary(reader);

            return affRange;
        }
        else
            return null;
    }

    /**
     * Parses the TRASH file name to extract the original path.
     *
     * @param name The TRASH short (entry) name.
     * @return The original path, or null in case of failure.
     */
    public static IgfsPath extractOriginalPathFromTrash(String name) {
        int idx = name.indexOf(TRASH_NAME_SEPARATOR);

        assert idx >= 0;

        String path = name.substring(idx + 1, name.length());

        return new IgfsPath(path);
    }

    /**
     * Creates short name of the file in TRASH directory.
     * The name consists of the whole file path and its unique id.
     * Upon file cleanup this name will be parsed to extract the path.
     * Note that in contrast to common practice the composed name contains '/' character.
     *
     * @param path The full path of the deleted file.
     * @param id The file id.
     * @return The new short name for trash directory.
     */
    static String composeNameForTrash(IgfsPath path, IgniteUuid id) {
        return id.toString() + TRASH_NAME_SEPARATOR + path.toString();
    }

    /**
     * Check whether provided node contains IGFS with the given name.
     *
     * @param node Node.
     * @param igfsName IGFS name.
     * @return {@code True} if it contains IGFS.
     */
    public static boolean isIgfsNode(ClusterNode node, String igfsName) {
        assert node != null;

        IgfsAttributes[] igfs = node.attribute(ATTR_IGFS);

        if (igfs != null)
            for (IgfsAttributes attrs : igfs)
                if (F.eq(igfsName, attrs.igfsName()))
                    return true;

        return false;
    }

    /**
     * Check whether mode is dual.
     *
     * @param mode Mode.
     * @return {@code True} if dual.
     */
    public static boolean isDualMode(IgfsMode mode) {
        return mode == DUAL_SYNC || mode == DUAL_ASYNC;
    }

    /**
     * Answers if directory of this mode can contain a subdirectory of the given mode.
     *
     * @param parent Parent mode.
     * @param child Child mode.
     * @return {@code true} if directory of this mode can contain a directory of the given mode.
     */
    public static boolean canContain(IgfsMode parent, IgfsMode child) {
        return isDualMode(parent) || parent == child;
    }

    /**
     * Checks, filters and sorts the modes.
     *
     * @param dfltMode The root mode. Must always be not null.
     * @param modes The subdirectory modes.
     * @param dualParentsContainingPrimaryChildren The set to store parents into.
     * @return Descending list of filtered and checked modes.
     * @throws IgniteCheckedException On error.
     */
    public static ArrayList<T2<IgfsPath, IgfsMode>> preparePathModes(final IgfsMode dfltMode,
        @Nullable List<T2<IgfsPath, IgfsMode>> modes, Set<IgfsPath> dualParentsContainingPrimaryChildren)
        throws IgniteCheckedException {
        if (modes == null)
            return null;

        // Sort by depth, shallow first.
        Collections.sort(modes, new Comparator<Map.Entry<IgfsPath, IgfsMode>>() {
            @Override public int compare(Map.Entry<IgfsPath, IgfsMode> o1, Map.Entry<IgfsPath, IgfsMode> o2) {
                return o1.getKey().depth() - o2.getKey().depth();
            }
        });

        ArrayList<T2<IgfsPath, IgfsMode>> resModes = new ArrayList<>(modes.size() + 1);

        resModes.add(new T2<>(IgfsPath.ROOT, dfltMode));

        for (T2<IgfsPath, IgfsMode> mode : modes) {
            assert mode.getKey() != null;

            for (T2<IgfsPath, IgfsMode> resMode : resModes) {
                if (mode.getKey().isSubDirectoryOf(resMode.getKey())) {
                    assert resMode.getValue() != null;

                    if (resMode.getValue() == mode.getValue())
                        // No reason to add a sub-path of the same mode, ignore this pair.
                        break;

                    if (!canContain(resMode.getValue(), mode.getValue()))
                        throw new IgniteCheckedException("Subdirectory " + mode.getKey() + " mode "
                            + mode.getValue() + " is not compatible with upper level "
                            + resMode.getKey() + " directory mode " + resMode.getValue() + ".");

                    // Add to the 1st position (deep first).
                    resModes.add(0, mode);

                    // Store primary paths inside dual paths in separate collection:
                    if (mode.getValue() == PRIMARY)
                        dualParentsContainingPrimaryChildren.add(mode.getKey().parent());

                    break;
                }
            }
        }

        // Remove root, because this class contract is that root mode is not contained in the list.
        resModes.remove(resModes.size() - 1);

        return resModes;
    }

    /**
     * Create flags value.
     *
     * @param isDir Directory flag.
     * @param isFile File flag.
     * @return Result.
     */
    public static byte flags(boolean isDir, boolean isFile) {
        byte res = isDir ? FLAG_DIR : 0;

        if (isFile)
            res |= FLAG_FILE;

        return res;
    }

    /**
     * Check whether passed flags represent directory.
     *
     * @param flags Flags.
     * @return {@code True} if this is directory.
     */
    public static boolean isDirectory(byte flags) {
        return hasFlag(flags, FLAG_DIR);
    }

    /**
     * Check whether passed flags represent file.
     *
     * @param flags Flags.
     * @return {@code True} if this is file.
     */
    public static boolean isFile(byte flags) {
        return hasFlag(flags, FLAG_FILE);
    }

    /**
     * Check whether certain flag is set.
     *
     * @param flags Flags.
     * @param flag Flag to check.
     * @return {@code True} if flag is set.
     */
    private static boolean hasFlag(byte flags, byte flag) {
        return (flags & flag) == flag;
    }

    /**
     * Reads string-to-string map written by {@link #writeStringMap(DataOutput, Map)}.
     *
     * @param in Data input.
     * @throws IOException If write failed.
     * @return Read result.
     */
    public static Map<String, String> readStringMap(DataInput in) throws IOException {
        int size = in.readInt();

        if (size == -1)
            return null;
        else {
            Map<String, String> map = U.newHashMap(size);

            for (int i = 0; i < size; i++)
                map.put(readUTF(in), readUTF(in));

            return map;
        }
    }

    /**
     * Writes string-to-string map to given data output.
     *
     * @param out Data output.
     * @param map Map.
     * @throws IOException If write failed.
     */
    public static void writeStringMap(DataOutput out, @Nullable Map<String, String> map) throws IOException {
        if (map != null) {
            out.writeInt(map.size());

            for (Map.Entry<String, String> e : map.entrySet()) {
                writeUTF(out, e.getKey());
                writeUTF(out, e.getValue());
            }
        }
        else
            out.writeInt(-1);
    }

    /**
     * Write UTF string which can be {@code null}.
     *
     * @param out Output stream.
     * @param val Value.
     * @throws IOException If failed.
     */
    public static void writeUTF(DataOutput out, @Nullable String val) throws IOException {
        if (val == null)
            out.writeInt(-1);
        else {
            out.writeInt(val.length());

            if (val.length() <= MAX_STR_LEN)
                out.writeUTF(val); // Optimized write in 1 chunk.
            else {
                int written = 0;

                while (written < val.length()) {
                    int partLen = Math.min(val.length() - written, MAX_STR_LEN);

                    String part = val.substring(written, written + partLen);

                    out.writeUTF(part);

                    written += partLen;
                }
            }
        }
    }

    /**
     * Read UTF string which can be {@code null}.
     *
     * @param in Input stream.
     * @return Value.
     * @throws IOException If failed.
     */
    public static String readUTF(DataInput in) throws IOException {
        int len = in.readInt(); // May be zero.

        if (len < 0)
            return null;
        else {
            if (len <= MAX_STR_LEN)
                return in.readUTF();

            StringBuilder sb = new StringBuilder(len);

            do {
                sb.append(in.readUTF());
            }
            while (sb.length() < len);

            assert sb.length() == len;

            return sb.toString();
        }
    }
}
