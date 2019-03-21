/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import static org.h2.mvstore.MVMap.INITIAL_VERSION;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.h2.compress.CompressDeflate;
import org.h2.compress.CompressLZF;
import org.h2.compress.Compressor;
import org.h2.engine.Constants;
import org.h2.mvstore.cache.CacheLongKeyLIRS;
import org.h2.util.MathUtils;
import org.h2.util.Utils;

/*

TODO:

Documentation
- rolling docs review: at "Metadata Map"
- better document that writes are in background thread
- better document how to do non-unique indexes
- document pluggable store and OffHeapStore

TransactionStore:
- ability to disable the transaction log,
    if there is only one connection

MVStore:
- better and clearer memory usage accounting rules
    (heap memory versus disk memory), so that even there is
    never an out of memory
    even for a small heap, and so that chunks
    are still relatively big on average
- make sure serialization / deserialization errors don't corrupt the file
- test and possibly improve compact operation (for large dbs)
- automated 'kill process' and 'power failure' test
- defragment (re-creating maps, specially those with small pages)
- store number of write operations per page (maybe defragment
    if much different than count)
- r-tree: nearest neighbor search
- use a small object value cache (StringCache), test on Android
    for default serialization
- MVStoreTool.dump should dump the data if possible;
    possibly using a callback for serialization
- implement a sharded map (in one store, multiple stores)
    to support concurrent updates and writes, and very large maps
- to save space when persisting very small transactions,
    use a transaction log where only the deltas are stored
- serialization for lists, sets, sets, sorted sets, maps, sorted maps
- maybe rename 'rollback' to 'revert' to distinguish from transactions
- support other compression algorithms (deflate, LZ4,...)
- remove features that are not really needed; simplify the code
    possibly using a separate layer or tools
    (retainVersion?)
- optional pluggable checksum mechanism (per page), which
    requires that everything is a page (including headers)
- rename "store" to "save", as "store" is used in "storeVersion"
- rename setStoreVersion to setDataVersion, setSchemaVersion or similar
- temporary file storage
- simple rollback method (rollback to last committed version)
- MVMap to implement SortedMap, then NavigableMap
- storage that splits database into multiple files,
    to speed up compact and allow using trim
    (by truncating / deleting empty files)
- add new feature to the file system API to avoid copying data
    (reads that returns a ByteBuffer instead of writing into one)
    for memory mapped files and off-heap storage
- support log structured merge style operations (blind writes)
    using one map per level plus bloom filter
- have a strict call order MVStore -> MVMap -> Page -> FileStore
- autocommit commits, stores, and compacts from time to time;
    the background thread should wait at least 90% of the
    configured write delay to store changes
- compact* should also store uncommitted changes (if there are any)
- write a LSM-tree (log structured merge tree) utility on top of the MVStore
    with blind writes and/or a bloom filter that
    internally uses regular maps and merge sort
- chunk metadata: maybe split into static and variable,
    or use a small page size for metadata
- data type "string": maybe use prefix compression for keys
- test chunk id rollover
- feature to auto-compact from time to time and on close
- compact very small chunks
- Page: to save memory, combine keys & values into one array
    (also children & counts). Maybe remove some other
    fields (childrenCount for example)
- Support SortedMap for MVMap
- compact: copy whole pages (without having to open all maps)
- maybe change the length code to have lower gaps
- test with very low limits (such as: short chunks, small pages)
- maybe allow to read beyond the retention time:
    when compacting, move live pages in old chunks
    to a map (possibly the metadata map) -
    this requires a change in the compaction code, plus
    a map lookup when reading old data; also, this
    old data map needs to be cleaned up somehow;
    maybe using an additional timeout
- rollback of removeMap should restore the data -
    which has big consequences, as the metadata map
    would probably need references to the root nodes of all maps

*/

/**
 * A persistent storage for maps.
 */
public class MVStore implements AutoCloseable {

    /**
     * The block size (physical sector size) of the disk. The store header is
     * written twice, one copy in each block, to ensure it survives a crash.
     */
    static final int BLOCK_SIZE = 4 * 1024;

    private static final int FORMAT_WRITE = 1;
    private static final int FORMAT_READ = 1;

    /**
     * Used to mark a chunk as free, when it was detected that live bookkeeping
     * is incorrect.
     */
    private static final int MARKED_FREE = 10_000_000;

    /**
     * Store is open.
     */
    private static final int STATE_OPEN = 0;

    /**
     * Store is about to close now, but is still operational.
     * Outstanding store operation by background writer or other thread may be in progress.
     * New updates must not be initiated, unless they are part of a closing procedure itself.
     */
    private static final int STATE_STOPPING = 1;

    /**
     * Store is closing now, and any operation on it may fail.
     */
    private static final int STATE_CLOSING = 2;

    /**
     * Store is closed.
     */
    private static final int STATE_CLOSED = 3;

    /**
     * Lock which governs access to major store operations: store(), close(), ...
     * It should used in a non-reentrant fashion.
     * It serves as a replacement for synchronized(this), except it allows for
     * non-blocking lock attempts.
     */
    private final ReentrantLock storeLock = new ReentrantLock(true);

    /**
     * Reference to a background thread, which is expected to be running, if any.
     */
    private final AtomicReference<BackgroundWriterThread> backgroundWriterThread = new AtomicReference<>();

    private volatile boolean reuseSpace = true;

    private volatile int state;

    private final FileStore fileStore;

    private final boolean fileStoreIsProvided;

    private final int pageSplitSize;

    private final int keysPerPage;

    /**
     * The page cache. The default size is 16 MB, and the average size is 2 KB.
     * It is split in 16 segments. The stack move distance is 2% of the expected
     * number of entries.
     */
    final CacheLongKeyLIRS<Page> cache;

    /**
     * The page chunk references cache. The default size is 4 MB, and the
     * average size is 2 KB. It is split in 16 segments. The stack move distance
     * is 2% of the expected number of entries.
     */
    final CacheLongKeyLIRS<int[]> cacheChunkRef;

    /**
     * The newest chunk. If nothing was stored yet, this field is not set.
     */
    private Chunk lastChunk;

    /**
     * The map of chunks.
     */
    private final ConcurrentHashMap<Integer, Chunk> chunks =
            new ConcurrentHashMap<>();

    private long updateCounter = 0;
    private long updateAttemptCounter = 0;

    /**
     * The map of temporarily freed storage space caused by freed pages.
     * It contains the number of freed entries per chunk.
     */
    private final Map<Integer, Chunk> freedPageSpace = new HashMap<>();

    /**
     * The metadata map. Write access to this map needs to be done under storeLock.
     */
    private final MVMap<String, String> meta;

    private final ConcurrentHashMap<Integer, MVMap<?, ?>> maps =
            new ConcurrentHashMap<>();

    private final HashMap<String, Object> storeHeader = new HashMap<>();

    private WriteBuffer writeBuffer;

    private final AtomicInteger lastMapId = new AtomicInteger();

    private int versionsToKeep = 5;

    /**
     * The compression level for new pages (0 for disabled, 1 for fast, 2 for
     * high). Even if disabled, the store may contain (old) compressed pages.
     */
    private final int compressionLevel;

    private Compressor compressorFast;

    private Compressor compressorHigh;

    private final UncaughtExceptionHandler backgroundExceptionHandler;

    private volatile long currentVersion;

    /**
     * The version of the last stored chunk, or -1 if nothing was stored so far.
     */
    private long lastStoredVersion = INITIAL_VERSION;

    /**
     * Oldest store version in use. All version beyond this can be safely dropped
     */
    private final AtomicLong oldestVersionToKeep = new AtomicLong();

    /**
     * Ordered collection of all version usage counters for all versions starting
     * from oldestVersionToKeep and up to current.
     */
    private final Deque<TxCounter> versions = new LinkedList<>();

    /**
     * Counter of open transactions for the latest (current) store version
     */
    private volatile TxCounter currentTxCounter = new TxCounter(currentVersion);

    /**
     * The estimated memory used by unsaved pages. This number is not accurate,
     * also because it may be changed concurrently, and because temporary pages
     * are counted.
     */
    private int unsavedMemory;
    private final int autoCommitMemory;
    private volatile boolean saveNeeded;

    /**
     * The time the store was created, in milliseconds since 1970.
     */
    private long creationTime;

    /**
     * How long to retain old, persisted chunks, in milliseconds. For larger or
     * equal to zero, a chunk is never directly overwritten if unused, but
     * instead, the unused field is set. If smaller zero, chunks are directly
     * overwritten if unused.
     */
    private int retentionTime;

    private long lastCommitTime;

    /**
     * The version of the current store operation (if any).
     */
    private volatile long currentStoreVersion = -1;

    private volatile boolean metaChanged;

    /**
     * The delay in milliseconds to automatically commit and write changes.
     */
    private int autoCommitDelay;

    private final int autoCompactFillRate;
    private long autoCompactLastFileOpCount;

    private volatile IllegalStateException panicException;

    private long lastTimeAbsolute;

    private long lastFreeUnusedChunks;

    /**
     * Create and open the store.
     *
     * @param config the configuration to use
     * @throws IllegalStateException if the file is corrupt, or an exception
     *             occurred while opening
     * @throws IllegalArgumentException if the directory does not exist
     */
    MVStore(Map<String, Object> config) {
        this.compressionLevel = DataUtils.getConfigParam(config, "compress", 0);
        String fileName = (String) config.get("fileName");
        FileStore fileStore = (FileStore) config.get("fileStore");
        fileStoreIsProvided = fileStore != null;
        if(fileStore == null && fileName != null) {
            fileStore = new FileStore();
        }
        this.fileStore = fileStore;

        int pgSplitSize = 48; // for "mem:" case it is # of keys
        CacheLongKeyLIRS.Config cc = null;
        if (this.fileStore != null) {
            int mb = DataUtils.getConfigParam(config, "cacheSize", 16);
            if (mb > 0) {
                cc = new CacheLongKeyLIRS.Config();
                cc.maxMemory = mb * 1024L * 1024L;
                Object o = config.get("cacheConcurrency");
                if (o != null) {
                    cc.segmentCount = (Integer)o;
                }
            }
            pgSplitSize = 16 * 1024;
        }
        if (cc != null) {
            cache = new CacheLongKeyLIRS<>(cc);
            cc.maxMemory /= 4;
            cacheChunkRef = new CacheLongKeyLIRS<>(cc);
        } else {
            cache = null;
            cacheChunkRef = null;
        }

        pgSplitSize = DataUtils.getConfigParam(config, "pageSplitSize", pgSplitSize);
        // Make sure pages will fit into cache
        if (cache != null && pgSplitSize > cache.getMaxItemSize()) {
            pgSplitSize = (int)cache.getMaxItemSize();
        }
        pageSplitSize = pgSplitSize;
        keysPerPage = DataUtils.getConfigParam(config, "keysPerPage", 48);
        backgroundExceptionHandler =
                (UncaughtExceptionHandler)config.get("backgroundExceptionHandler");
        meta = new MVMap<>(this);
        if (this.fileStore != null) {
            retentionTime = this.fileStore.getDefaultRetentionTime();
            // 19 KB memory is about 1 KB storage
            int kb = Math.max(1, Math.min(19, Utils.scaleForAvailableMemory(64))) * 1024;
            kb = DataUtils.getConfigParam(config, "autoCommitBufferSize", kb);
            autoCommitMemory = kb * 1024;
            autoCompactFillRate = DataUtils.getConfigParam(config, "autoCompactFillRate", 40);
            char[] encryptionKey = (char[]) config.get("encryptionKey");
            try {
                if (!fileStoreIsProvided) {
                    boolean readOnly = config.containsKey("readOnly");
                    this.fileStore.open(fileName, readOnly, encryptionKey);
                }
                if (this.fileStore.size() == 0) {
                    creationTime = getTimeAbsolute();
                    lastCommitTime = creationTime;
                    storeHeader.put("H", 2);
                    storeHeader.put("blockSize", BLOCK_SIZE);
                    storeHeader.put("format", FORMAT_WRITE);
                    storeHeader.put("created", creationTime);
                    writeStoreHeader();
                } else {
                    readStoreHeader();
                }
            } catch (IllegalStateException e) {
                panic(e);
            } finally {
                if (encryptionKey != null) {
                    Arrays.fill(encryptionKey, (char) 0);
                }
            }
            lastCommitTime = getTimeSinceCreation();

            Set<String> rootsToRemove = new HashSet<>();
            for (Iterator<String> it = meta.keyIterator("root."); it.hasNext();) {
                String key = it.next();
                if (!key.startsWith("root.")) {
                    break;
                }
                String mapId = key.substring(key.lastIndexOf('.') + 1);
                if(!meta.containsKey("map."+mapId)) {
                    rootsToRemove.add(key);
                }
            }

            for (String key : rootsToRemove) {
                meta.remove(key);
                markMetaChanged();
            }

            // setAutoCommitDelay starts the thread, but only if
            // the parameter is different from the old value
            int delay = DataUtils.getConfigParam(config, "autoCommitDelay", 1000);
            setAutoCommitDelay(delay);
        } else {
            autoCommitMemory = 0;
            autoCompactFillRate = 0;
        }
    }

    private void panic(IllegalStateException e) {
        if (isOpen()) {
            handleException(e);
            panicException = e;
            closeImmediately();
        }
        throw e;
    }

    public IllegalStateException getPanicException() {
        return panicException;
    }

    /**
     * Open a store in exclusive mode. For a file-based store, the parent
     * directory must already exist.
     *
     * @param fileName the file name (null for in-memory)
     * @return the store
     */
    public static MVStore open(String fileName) {
        HashMap<String, Object> config = new HashMap<>();
        config.put("fileName", fileName);
        return new MVStore(config);
    }

    /**
     * Find position of the root page for historical version of the map.
     *
     * @param mapId to find the old version for
     * @param version the version
     * @return position of the root Page
     */
    long getRootPos(int mapId, long version) {
        MVMap<String, String> oldMeta = getMetaMap(version);
        return getRootPos(oldMeta, mapId);
    }

    /**
     * Open a map with the default settings. The map is automatically create if
     * it does not yet exist. If a map with this name is already open, this map
     * is returned.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param name the name of the map
     * @return the map
     */
    public <K, V> MVMap<K, V> openMap(String name) {
        return openMap(name, new MVMap.Builder<K, V>());
    }

    /**
     * Open a map with the given builder. The map is automatically create if it
     * does not yet exist. If a map with this name is already open, this map is
     * returned.
     *
     * @param <M> the map type
     * @param <K> the key type
     * @param <V> the value type
     * @param name the name of the map
     * @param builder the map builder
     * @return the map
     */
    public <M extends MVMap<K, V>, K, V> M openMap(String name, MVMap.MapBuilder<M, K, V> builder) {
        int id = getMapId(name);
        M map;
        if (id >= 0) {
            map = openMap(id, builder);
            assert builder.getKeyType() == null || map.getKeyType().getClass().equals(builder.getKeyType().getClass());
            assert builder.getValueType() == null || map.getValueType().getClass().equals(builder.getValueType()
                    .getClass());
        } else {
            HashMap<String, Object> c = new HashMap<>();
            id = lastMapId.incrementAndGet();
            assert getMap(id) == null;
            c.put("id", id);
            c.put("createVersion", currentVersion);
            map = builder.create(this, c);
            String x = Integer.toHexString(id);
            meta.put(MVMap.getMapKey(id), map.asString(name));
            meta.put("name." + name, x);
            map.setRootPos(0, lastStoredVersion);
            markMetaChanged();
            @SuppressWarnings("unchecked")
            M existingMap = (M) maps.putIfAbsent(id, map);
            if (existingMap != null) {
                map = existingMap;
            }
        }
        return map;
    }

    private <M extends MVMap<K, V>, K, V> M openMap(int id, MVMap.MapBuilder<M, K, V> builder) {
        storeLock.lock();
        try {
            @SuppressWarnings("unchecked")
            M map = (M) getMap(id);
            if (map == null) {
                String configAsString = meta.get(MVMap.getMapKey(id));
                HashMap<String, Object> config;
                if (configAsString != null) {
                    config = new HashMap<String, Object>(DataUtils.parseMap(configAsString));
                } else {
                    config = new HashMap<>();
                }
                config.put("id", id);
                map = builder.create(this, config);
                long root = getRootPos(meta, id);
                map.setRootPos(root, lastStoredVersion);
                maps.put(id, map);
            }
            return map;
        } finally {
            storeLock.unlock();
        }
    }

    /**
     * Get map by id.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param id map id
     * @return Map
     */
    public <K, V> MVMap<K,V> getMap(int id) {
        checkOpen();
        @SuppressWarnings("unchecked")
        MVMap<K, V> map = (MVMap<K, V>) maps.get(id);
        return map;
    }

    /**
     * Get the set of all map names.
     *
     * @return the set of names
     */
    public Set<String> getMapNames() {
        HashSet<String> set = new HashSet<>();
        checkOpen();
        for (Iterator<String> it = meta.keyIterator("name."); it.hasNext();) {
            String x = it.next();
            if (!x.startsWith("name.")) {
                break;
            }
            String mapName = x.substring("name.".length());
            set.add(mapName);
        }
        return set;
    }

    /**
     * Get the metadata map. This data is for informational purposes only. The
     * data is subject to change in future versions.
     * <p>
     * The data in this map should not be modified (changing system data may
     * corrupt the store). If modifications are needed, they need be
     * synchronized on the store.
     * <p>
     * The metadata map contains the following entries:
     * <pre>
     * chunk.{chunkId} = {chunk metadata}
     * name.{name} = {mapId}
     * map.{mapId} = {map metadata}
     * root.{mapId} = {root position}
     * setting.storeVersion = {version}
     * </pre>
     *
     * @return the metadata map
     */
    public MVMap<String, String> getMetaMap() {
        checkOpen();
        return meta;
    }

    private MVMap<String, String> getMetaMap(long version) {
        Chunk c = getChunkForVersion(version);
        DataUtils.checkArgument(c != null, "Unknown version {0}", version);
        c = readChunkHeader(c.block);
        MVMap<String, String> oldMeta = meta.openReadOnly(c.metaRootPos, version);
        return oldMeta;
    }

    private Chunk getChunkForVersion(long version) {
        Chunk newest = null;
        for (Chunk c : chunks.values()) {
            if (c.version <= version) {
                if (newest == null || c.id > newest.id) {
                    newest = c;
                }
            }
        }
        return newest;
    }

    /**
     * Check whether a given map exists.
     *
     * @param name the map name
     * @return true if it exists
     */
    public boolean hasMap(String name) {
        return meta.containsKey("name." + name);
    }

    /**
     * Check whether a given map exists and has data.
     *
     * @param name the map name
     * @return true if it exists and has data.
     */
    public boolean hasData(String name) {
        return hasMap(name) && getRootPos(meta, getMapId(name)) != 0;
    }

    private void markMetaChanged() {
        // changes in the metadata alone are usually not detected, as the meta
        // map is changed after storing
        metaChanged = true;
    }

    private void readStoreHeader() {
        Chunk newest = null;
        boolean validStoreHeader = false;
        // find out which chunk and version are the newest
        // read the first two blocks
        ByteBuffer fileHeaderBlocks = fileStore.readFully(0, 2 * BLOCK_SIZE);
        byte[] buff = new byte[BLOCK_SIZE];
        for (int i = 0; i <= BLOCK_SIZE; i += BLOCK_SIZE) {
            fileHeaderBlocks.get(buff);
            // the following can fail for various reasons
            try {
                HashMap<String, String> m = DataUtils.parseChecksummedMap(buff);
                if (m == null) {
                    continue;
                }
                int blockSize = DataUtils.readHexInt(
                        m, "blockSize", BLOCK_SIZE);
                if (blockSize != BLOCK_SIZE) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_UNSUPPORTED_FORMAT,
                            "Block size {0} is currently not supported",
                            blockSize);
                }
                long version = DataUtils.readHexLong(m, "version", 0);
                if (newest == null || version > newest.version) {
                    validStoreHeader = true;
                    storeHeader.putAll(m);
                    creationTime = DataUtils.readHexLong(m, "created", 0);
                    int chunkId = DataUtils.readHexInt(m, "chunk", 0);
                    long block = DataUtils.readHexLong(m, "block", 0);
                    Chunk test = readChunkHeaderAndFooter(block);
                    if (test != null && test.id == chunkId) {
                        newest = test;
                    }
                }
            } catch (Exception ignore) {/**/}
        }
        if (!validStoreHeader) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "Store header is corrupt: {0}", fileStore);
        }
        long format = DataUtils.readHexLong(storeHeader, "format", 1);
        if (format > FORMAT_WRITE && !fileStore.isReadOnly()) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    "The write format {0} is larger " +
                    "than the supported format {1}, " +
                    "and the file was not opened in read-only mode",
                    format, FORMAT_WRITE);
        }
        format = DataUtils.readHexLong(storeHeader, "formatRead", format);
        if (format > FORMAT_READ) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    "The read format {0} is larger " +
                    "than the supported format {1}",
                    format, FORMAT_READ);
        }
        lastStoredVersion = INITIAL_VERSION;
        chunks.clear();
        long now = System.currentTimeMillis();
        // calculate the year (doesn't have to be exact;
        // we assume 365.25 days per year, * 4 = 1461)
        int year =  1970 + (int) (now / (1000L * 60 * 60 * 6 * 1461));
        if (year < 2014) {
            // if the year is before 2014,
            // we assume the system doesn't have a real-time clock,
            // and we set the creationTime to the past, so that
            // existing chunks are overwritten
            creationTime = now - fileStore.getDefaultRetentionTime();
        } else if (now < creationTime) {
            // the system time was set to the past:
            // we change the creation time
            creationTime = now;
            storeHeader.put("created", creationTime);
        }
        Chunk test = readChunkFooter(fileStore.size());
        if (test != null) {
            test = readChunkHeaderAndFooter(test.block);
            if (test != null) {
                if (newest == null || test.version > newest.version) {
                    newest = test;
                }
            }
        }

        long blocksInStore = fileStore.size() / BLOCK_SIZE;
        // this queue will hold potential candidates for lastChunk to fall back to
        Queue<Chunk> lastChunkCandidates = new PriorityQueue<>(Math.max(32, (int)(blocksInStore / 4)),
                new Comparator<Chunk>() {
            @Override
            public int compare(Chunk one, Chunk two) {
                int result = Long.compare(two.version, one.version);
                if (result == 0) {
                    // out of two versions of the same chunk we prefer the one
                    // close to the beginning of file (presumably later version)
                    result = Long.compare(one.block, two.block);
                }
                return result;
            }
        });
        Map<Long, Chunk> validChunkCacheByLocation = new HashMap<>();

        if (newest != null) {
            // read the chunk header and footer,
            // and follow the chain of next chunks
            while (true) {
                validChunkCacheByLocation.put(newest.block, newest);
                lastChunkCandidates.add(newest);
                if (newest.next == 0 ||
                        newest.next >= blocksInStore) {
                    // no (valid) next
                    break;
                }
                test = readChunkHeaderAndFooter(newest.next);
                if (test == null || test.id <= newest.id) {
                    break;
                }
                newest = test;
            }
        }

        // Try candidates for "last chunk" in order from newest to oldest
        // until suitable is found. Suitable one should have meta map
        // where all chunk references point to valid locations.
        boolean verified = false;
        while(!verified && setLastChunk(lastChunkCandidates.poll()) != null) {
            verified = true;
            // load the chunk metadata: although meta's root page resides in the lastChunk,
            // traversing meta map might recursively load another chunk(s)
            Cursor<String, String> cursor = meta.cursor("chunk.");
            while (cursor.hasNext() && cursor.next().startsWith("chunk.")) {
                Chunk c = Chunk.fromString(cursor.getValue());
                assert c.version <= currentVersion;
                // might be there already, due to meta traversal
                // see readPage() ... getChunkIfFound()
                chunks.putIfAbsent(c.id, c);
                long block = c.block;
                test = validChunkCacheByLocation.get(block);
                if (test == null) {
                    test = readChunkHeaderAndFooter(block);
                    if (test != null && test.id == c.id) { // chunk is valid
                        validChunkCacheByLocation.put(block, test);
                        lastChunkCandidates.offer(test);
                        continue;
                    }
                } else if (test.id == c.id) { // chunk is valid
                    // nothing to do, since chunk was already verified
                    // and registered as potential "last chunk" candidate
                    continue;
                }
                // chunk reference is invalid
                // this "last chunk" candidate is not suitable
                // but we continue to process all references
                // to find other potential candidates
                verified = false;
            }
        }

        fileStore.clear();
        // build the free space list
        for (Chunk c : chunks.values()) {
            long start = c.block * BLOCK_SIZE;
            int length = c.len * BLOCK_SIZE;
            fileStore.markUsed(start, length);
        }
        assert fileStore.getFileLengthInUse() == measureFileLengthInUse() :
                fileStore.getFileLengthInUse() + " != " + measureFileLengthInUse();
        setWriteVersion(currentVersion);
        if (lastStoredVersion == INITIAL_VERSION) {
            lastStoredVersion = currentVersion - 1;
        }
    }

    private Chunk setLastChunk(Chunk last) {
        chunks.clear();
        lastChunk = last;
        if (last == null) {
            // no valid chunk
            lastMapId.set(0);
            currentVersion = 0;
            lastStoredVersion = INITIAL_VERSION;
            meta.setRootPos(0, INITIAL_VERSION);
        } else {
            lastMapId.set(last.mapId);
            currentVersion = last.version;
            chunks.put(last.id, last);
            lastStoredVersion = currentVersion - 1;
            meta.setRootPos(last.metaRootPos, lastStoredVersion);
        }
        return last;
    }


    /**
     * Read a chunk header and footer, and verify the stored data is consistent.
     *
     * @param block the block
     * @return the chunk, or null if the header or footer don't match or are not
     *         consistent
     */
    private Chunk readChunkHeaderAndFooter(long block) {
        Chunk header;
        try {
            header = readChunkHeader(block);
        } catch (Exception e) {
            // invalid chunk header: ignore, but stop
            return null;
        }
        if (header == null) {
            return null;
        }
        Chunk footer = readChunkFooter((block + header.len) * BLOCK_SIZE);
        if (footer == null || footer.id != header.id) {
            return null;
        }
        return header;
    }

    /**
     * Try to read a chunk footer.
     *
     * @param end the end of the chunk
     * @return the chunk, or null if not successful
     */
    private Chunk readChunkFooter(long end) {
        // the following can fail for various reasons
        try {
            // read the chunk footer of the last block of the file
            long pos = end - Chunk.FOOTER_LENGTH;
            if(pos < 0) {
                return null;
            }
            ByteBuffer lastBlock = fileStore.readFully(pos, Chunk.FOOTER_LENGTH);
            byte[] buff = new byte[Chunk.FOOTER_LENGTH];
            lastBlock.get(buff);
            HashMap<String, String> m = DataUtils.parseChecksummedMap(buff);
            if (m != null) {
                int chunk = DataUtils.readHexInt(m, "chunk", 0);
                Chunk c = new Chunk(chunk);
                c.version = DataUtils.readHexLong(m, "version", 0);
                c.block = DataUtils.readHexLong(m, "block", 0);
                return c;
            }
        } catch (Exception e) {
            // ignore
        }
        return null;
    }

    private void writeStoreHeader() {
        StringBuilder buff = new StringBuilder(112);
        if (lastChunk != null) {
            storeHeader.put("block", lastChunk.block);
            storeHeader.put("chunk", lastChunk.id);
            storeHeader.put("version", lastChunk.version);
        }
        DataUtils.appendMap(buff, storeHeader);
        byte[] bytes = buff.toString().getBytes(StandardCharsets.ISO_8859_1);
        int checksum = DataUtils.getFletcher32(bytes, 0, bytes.length);
        DataUtils.appendMap(buff, "fletcher", checksum);
        buff.append('\n');
        bytes = buff.toString().getBytes(StandardCharsets.ISO_8859_1);
        ByteBuffer header = ByteBuffer.allocate(2 * BLOCK_SIZE);
        header.put(bytes);
        header.position(BLOCK_SIZE);
        header.put(bytes);
        header.rewind();
        write(0, header);
    }

    private void write(long pos, ByteBuffer buffer) {
        try {
            fileStore.writeFully(pos, buffer);
        } catch (IllegalStateException e) {
            panic(e);
        }
    }

    /**
     * Close the file and the store. Unsaved changes are written to disk first.
     */
    @Override
    public void close() {
        closeStore(true);
    }

    /**
     * Close the file and the store, without writing anything.
     * This will try to stop the background thread (without waiting for it).
     * This method ignores all errors.
     */
    public void closeImmediately() {
        try {
            closeStore(false);
        } catch (Throwable e) {
            handleException(e);
        }
    }

    private void closeStore(boolean normalShutdown) {
        // If any other thead have already initiated closure procedure,
        // isClosed() would wait until closure is done and then  we jump out of the loop.
        // This is a subtle difference between !isClosed() and isOpen().
        while (!isClosed()) {
            stopBackgroundThread(normalShutdown);
            storeLock.lock();
            try {
                if (state == STATE_OPEN) {
                    state = STATE_STOPPING;
                    try {
                        try {
                            if (normalShutdown && fileStore != null && !fileStore.isReadOnly()) {
                                for (MVMap<?, ?> map : maps.values()) {
                                    if (map.isClosed()) {
                                        if (meta.remove(MVMap.getMapRootKey(map.getId())) != null) {
                                            markMetaChanged();
                                        }
                                    }
                                }
                                commit();

                                shrinkFileIfPossible(0);
                            }

                            state = STATE_CLOSING;

                            // release memory early - this is important when called
                            // because of out of memory
                            if (cache != null) {
                                cache.clear();
                            }
                            if (cacheChunkRef != null) {
                                cacheChunkRef.clear();
                            }
                            for (MVMap<?, ?> m : new ArrayList<>(maps.values())) {
                                m.close();
                            }
                            chunks.clear();
                            maps.clear();
                        } finally {
                            if (fileStore != null && !fileStoreIsProvided) {
                                fileStore.close();
                            }
                        }
                    } finally {
                        state = STATE_CLOSED;
                    }
                }
            } finally {
                storeLock.unlock();
            }
        }
    }

    /**
     * Read a page of data into a ByteBuffer.
     *
     * @param pos page pos
     * @param expectedMapId expected map id for the page
     * @return ByteBuffer containing page data.
     */
    ByteBuffer readBufferForPage(long pos, int expectedMapId) {
        Chunk c = getChunk(pos);
        long filePos = c.block * BLOCK_SIZE;
        filePos += DataUtils.getPageOffset(pos);
        if (filePos < 0) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "Negative position {0}; p={1}, c={2}", filePos, pos, c.toString());
        }
        long maxPos = (c.block + c.len) * BLOCK_SIZE;

        ByteBuffer buff;
        int maxLength = DataUtils.getPageMaxLength(pos);
        if (maxLength == DataUtils.PAGE_LARGE) {
            buff = fileStore.readFully(filePos, 128);
            maxLength = buff.getInt();
            // read the first bytes again
        }
        maxLength = (int) Math.min(maxPos - filePos, maxLength);
        int length = maxLength;
        if (length < 0) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "Illegal page length {0} reading at {1}; max pos {2} ", length, filePos, maxPos);
        }
        buff = fileStore.readFully(filePos, length);
        int chunkId = DataUtils.getPageChunkId(pos);
        int offset = DataUtils.getPageOffset(pos);
        int start = buff.position();
        int remaining = buff.remaining();
        int pageLength = buff.getInt();
        if (pageLength > remaining || pageLength < 4) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected page length 4..{1}, got {2}", chunkId, remaining,
                    pageLength);
        }
        buff.limit(start + pageLength);

        short check = buff.getShort();
        int mapId = DataUtils.readVarInt(buff);
        if (mapId != expectedMapId) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected map id {1}, got {2}", chunkId, expectedMapId, mapId);
        }
        int checkTest = DataUtils.getCheckValue(chunkId)
                ^ DataUtils.getCheckValue(offset)
                ^ DataUtils.getCheckValue(pageLength);
        if (check != (short) checkTest) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected check value {1}, got {2}", chunkId, checkTest, check);
        }
        return buff;
    }

    /**
     * Get the chunk for the given position.
     *
     * @param pos the position
     * @return the chunk
     */
    private Chunk getChunk(long pos) {
        int chunkId = DataUtils.getPageChunkId(pos);
        Chunk c = chunks.get(chunkId);
        if (c == null) {
            checkOpen();
            String s = meta.get(Chunk.getMetaKey(chunkId));
            if (s == null) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_CHUNK_NOT_FOUND,
                        "Chunk {0} not found", chunkId);
            }
            c = Chunk.fromString(s);
            if (c.block == Long.MAX_VALUE) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_CORRUPT,
                        "Chunk {0} is invalid", chunkId);
            }
            chunks.put(c.id, c);
        }
        return c;
    }

    private void setWriteVersion(long version) {
        for (Iterator<MVMap<?, ?>> iter = maps.values().iterator(); iter.hasNext(); ) {
            MVMap<?, ?> map = iter.next();
            if (map.setWriteVersion(version) == null) {
                assert map.isClosed();
                assert map.getVersion() < getOldestVersionToKeep();
                meta.remove(MVMap.getMapRootKey(map.getId()));
                markMetaChanged();
                iter.remove();
            }
        }
        meta.setWriteVersion(version);
        onVersionChange(version);
    }

    /**
     * Unlike regular commit this method returns immediately if there is commit
     * in progress on another thread, otherwise it acts as regular commit.
     *
     * This method may return BEFORE this thread changes are actually persisted!
     *
     * @return the new version (incremented if there were changes)
     */
    public long tryCommit() {
        // we need to prevent re-entrance, which may be possible,
        // because meta map is modified within storeNow() and that
        // causes beforeWrite() call with possibility of going back here
        if ((!storeLock.isHeldByCurrentThread() || currentStoreVersion < 0) &&
                storeLock.tryLock()) {
            try {
                store();
            } finally {
                storeLock.unlock();
            }
        }
        return currentVersion;
    }

    /**
     * Commit the changes.
     * <p>
     * This method does nothing if there are no unsaved changes,
     * otherwise it increments the current version
     * and stores the data (for file based stores).
     * <p>
     * It is not necessary to call this method when auto-commit is enabled (the default
     * setting), as in this case it is automatically called from time to time or
     * when enough changes have accumulated. However, it may still be called to
     * flush all changes to disk.
     * <p>
     * At most one store operation may run at any time.
     *
     * @return the new version (incremented if there were changes)
     */
    public long commit() {
        // we need to prevent re-entrance, which may be possible,
        // because meta map is modified within storeNow() and that
        // causes beforeWrite() call with possibility of going back here
        if(!storeLock.isHeldByCurrentThread() || currentStoreVersion < 0) {
            storeLock.lock();
            try {
                store();
            } finally {
                storeLock.unlock();
            }
        }
        return currentVersion;
    }

    private void store() {
        try {
            if (isOpenOrStopping() && hasUnsavedChangesInternal()) {
                currentStoreVersion = currentVersion;
                if (fileStore == null) {
                    lastStoredVersion = currentVersion;
                    //noinspection NonAtomicOperationOnVolatileField
                    ++currentVersion;
                    setWriteVersion(currentVersion);
                    metaChanged = false;
                } else {
                    if (fileStore.isReadOnly()) {
                        throw DataUtils.newIllegalStateException(
                                DataUtils.ERROR_WRITING_FAILED, "This store is read-only");
                    }
                    try {
                        storeNow();
                    } catch (IllegalStateException e) {
                        panic(e);
                    } catch (Throwable e) {
                        panic(DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, e.toString(), e));
                    }
                }
            }
        } finally {
            // in any case reset the current store version,
            // to allow closing the store
            currentStoreVersion = -1;
        }
    }

    private void storeNow() {
        assert storeLock.isHeldByCurrentThread();
        long time = getTimeSinceCreation();
        freeUnusedIfNeeded(time);
        int currentUnsavedPageCount = unsavedMemory;
        long storeVersion = currentStoreVersion;
        long version = ++currentVersion;
        lastCommitTime = time;

        // the metadata of the last chunk was not stored so far, and needs to be
        // set now (it's better not to update right after storing, because that
        // would modify the meta map again)
        int lastChunkId;
        if (lastChunk == null) {
            lastChunkId = 0;
        } else {
            lastChunkId = lastChunk.id;
            meta.put(Chunk.getMetaKey(lastChunkId), lastChunk.asString());
            markMetaChanged();
            // never go backward in time
            time = Math.max(lastChunk.time, time);
        }
        int newChunkId = lastChunkId;
        while (true) {
            newChunkId = (newChunkId + 1) & Chunk.MAX_ID;
            Chunk old = chunks.get(newChunkId);
            if (old == null) {
                break;
            }
            if (old.block == Long.MAX_VALUE) {
                IllegalStateException e = DataUtils.newIllegalStateException(
                        DataUtils.ERROR_INTERNAL,
                        "Last block {0} not stored, possibly due to out-of-memory", old);
                panic(e);
            }
        }
        Chunk c = new Chunk(newChunkId);
        c.pageCount = Integer.MAX_VALUE;
        c.pageCountLive = Integer.MAX_VALUE;
        c.maxLen = Long.MAX_VALUE;
        c.maxLenLive = Long.MAX_VALUE;
        c.metaRootPos = Long.MAX_VALUE;
        c.block = Long.MAX_VALUE;
        c.len = Integer.MAX_VALUE;
        c.time = time;
        c.version = version;
        c.mapId = lastMapId.get();
        c.next = Long.MAX_VALUE;
        chunks.put(c.id, c);
        ArrayList<Page> changed = new ArrayList<>();
        for (Iterator<MVMap<?, ?>> iter = maps.values().iterator(); iter.hasNext(); ) {
            MVMap<?, ?> map = iter.next();
            RootReference rootReference = map.setWriteVersion(version);
            if (rootReference == null) {
                assert map.isClosed();
                assert map.getVersion() < getOldestVersionToKeep();
                meta.remove(MVMap.getMapRootKey(map.getId()));
                iter.remove();
            } else if (map.getCreateVersion() <= storeVersion && // if map was created after storing started, skip it
                    !map.isVolatile() &&
                    map.hasChangesSince(lastStoredVersion)) {
                assert rootReference.version <= version : rootReference.version + " > " + version;
                Page rootPage = rootReference.root;
                if (!rootPage.isSaved() ||
                        // after deletion previously saved leaf
                        // may pop up as a root, but we still need
                        // to save new root pos in meta
                        rootPage.isLeaf()) {
                    changed.add(rootPage);
                }
            }
        }
        WriteBuffer buff = getWriteBuffer();
        // need to patch the header later
        c.writeChunkHeader(buff, 0);
        int headerLength = buff.position();
        c.pageCount = 0;
        c.pageCountLive = 0;
        c.maxLen = 0;
        c.maxLenLive = 0;
        for (Page p : changed) {
            String key = MVMap.getMapRootKey(p.getMapId());
            if (p.getTotalCount() == 0) {
                meta.remove(key);
            } else {
                p.writeUnsavedRecursive(c, buff);
                long root = p.getPos();
                meta.put(key, Long.toHexString(root));
            }
        }
        applyFreedSpace();
        RootReference metaRootReference = meta.setWriteVersion(version);
        assert metaRootReference != null;
        assert metaRootReference.version == version : metaRootReference.version + " != " + version;
        metaChanged = false;
        onVersionChange(version);

        Page metaRoot = metaRootReference.root;
        metaRoot.writeUnsavedRecursive(c, buff);

        int chunkLength = buff.position();

        // add the store header and round to the next block
        int length = MathUtils.roundUpInt(chunkLength +
                Chunk.FOOTER_LENGTH, BLOCK_SIZE);
        buff.limit(length);

        long filePos = allocateFileSpace(length, !reuseSpace);
        c.block = filePos / BLOCK_SIZE;
        c.len = length / BLOCK_SIZE;
        assert fileStore.getFileLengthInUse() == measureFileLengthInUse() :
                fileStore.getFileLengthInUse() + " != " + measureFileLengthInUse() + " " + c;
        c.metaRootPos = metaRoot.getPos();
        // calculate and set the likely next position
        if (reuseSpace) {
            c.next = fileStore.predictAllocation(c.len * BLOCK_SIZE) / BLOCK_SIZE;
        } else {
            // just after this chunk
            c.next = 0;
        }
        buff.position(0);
        c.writeChunkHeader(buff, headerLength);

        buff.position(buff.limit() - Chunk.FOOTER_LENGTH);
        buff.put(c.getFooterBytes());

        buff.position(0);
        write(filePos, buff.getBuffer());
        releaseWriteBuffer(buff);

        // whether we need to write the store header
        boolean writeStoreHeader = false;
        // end of the used space is not necessarily the end of the file
        boolean storeAtEndOfFile = filePos + length >= fileStore.size();
        if (!storeAtEndOfFile) {
            if (lastChunk == null) {
                writeStoreHeader = true;
            } else if (lastChunk.next != c.block) {
                // the last prediction did not matched
                writeStoreHeader = true;
            } else {
                long headerVersion = DataUtils.readHexLong(
                        storeHeader, "version", 0);
                if (lastChunk.version - headerVersion > 20) {
                    // we write after at least every 20 versions
                    writeStoreHeader = true;
                } else {
                    int chunkId = DataUtils.readHexInt(storeHeader, "chunk", 0);
                    while (true) {
                        Chunk old = chunks.get(chunkId);
                        if (old == null) {
                            // one of the chunks in between
                            // was removed
                            writeStoreHeader = true;
                            break;
                        }
                        if (chunkId == lastChunk.id) {
                            break;
                        }
                        chunkId++;
                    }
                }
            }
        }

        lastChunk = c;
        if (writeStoreHeader) {
            writeStoreHeader();
        }
        if (!storeAtEndOfFile) {
            // may only shrink after the store header was written
            shrinkFileIfPossible(1);
        }
        for (Page p : changed) {
            p.writeEnd();
        }
        metaRoot.writeEnd();

        // some pages might have been changed in the meantime (in the newest
        // version)
        unsavedMemory = Math.max(0, unsavedMemory
                - currentUnsavedPageCount);

        lastStoredVersion = storeVersion;
    }

    /**
     * Try to free unused chunks. This method doesn't directly write, but can
     * change the metadata, and therefore cause a background write.
     */
    private void freeUnusedIfNeeded(long time) {
        int freeDelay = retentionTime / 5;
        if (time - lastFreeUnusedChunks >= freeDelay) {
            // set early in case it fails (out of memory or so)
            lastFreeUnusedChunks = time;
            freeUnusedChunks(true);
        }
    }

    private void freeUnusedChunks(boolean fast) {
        assert storeLock.isHeldByCurrentThread();
        if (lastChunk != null && reuseSpace) {
            Set<Integer> referenced = collectReferencedChunks(fast);
            long time = getTimeSinceCreation();

            for (Iterator<Chunk> iterator = chunks.values().iterator(); iterator.hasNext(); ) {
                Chunk c = iterator.next();
                if (c.block != Long.MAX_VALUE && !referenced.contains(c.id)) {
                    if (canOverwriteChunk(c, time)) {
                        iterator.remove();
                        if (meta.remove(Chunk.getMetaKey(c.id)) != null) {
                            markMetaChanged();
                        }
                        long start = c.block * BLOCK_SIZE;
                        int length = c.len * BLOCK_SIZE;
                        fileStore.free(start, length);
                        assert fileStore.getFileLengthInUse() == measureFileLengthInUse() :
                                fileStore.getFileLengthInUse() + " != " + measureFileLengthInUse();
                    } else {
                        if (c.unused == 0) {
                            c.unused = time;
                            meta.put(Chunk.getMetaKey(c.id), c.asString());
                            markMetaChanged();
                        }
                    }
                }
            }
            // set it here, to avoid calling it often if it was slow
            lastFreeUnusedChunks = getTimeSinceCreation();
        }
    }

    /**
     * Collect ids for chunks that are in use.
     * @param fast if true, simplified version is used, which assumes that recent chunks
     *            are still in-use and do not scan recent versions of the store.
     *            Also is this case only oldest available version of the store is scanned.
     * @return set of chunk ids in-use, or null if all chunks should be considered in-use
     */
    private Set<Integer> collectReferencedChunks(boolean fast) {
        assert lastChunk != null;
        final ThreadPoolExecutor executorService = new ThreadPoolExecutor(10, 10, 10L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(keysPerPage + 1));
        final AtomicInteger executingThreadCounter = new AtomicInteger();
        try {
            ChunkIdsCollector collector = new ChunkIdsCollector(meta.getId());
            long oldestVersionToKeep = getOldestVersionToKeep();
            RootReference rootReference = meta.flushAndGetRoot();
            if (fast) {
                RootReference previous;
                while (rootReference.version >= oldestVersionToKeep && (previous = rootReference.previous) != null) {
                    rootReference = previous;
                }
                inspectVersion(rootReference, collector, executorService, executingThreadCounter, null);

                Page rootPage = rootReference.root;
                long pos = rootPage.getPos();
                assert rootPage.isSaved();
                int chunkId = DataUtils.getPageChunkId(pos);
                while (++chunkId <= lastChunk.id) {
                    collector.registerChunk(chunkId);
                }
            } else {
                Set<Long> inspectedRoots = new HashSet<>();
                do {
                    inspectVersion(rootReference, collector, executorService, executingThreadCounter, inspectedRoots);
                } while (rootReference.version >= oldestVersionToKeep
                        && (rootReference = rootReference.previous) != null);
            }
            return collector.getReferenced();
        } finally {
            executorService.shutdownNow();
        }
    }

    /**
     * Scans all map of a particular store version and marks visited chunks as in-use.
     * @param rootReference of the meta map of the version
     * @param collector to report visited chunks to
     * @param executorService to use for parallel processing
     * @param executingThreadCounter counter for threads already in use
     * @param inspectedRoots set of page positions for map's roots already inspected
     *                      or null if not to be used
     */
    private void inspectVersion(RootReference rootReference, ChunkIdsCollector collector,
                                ThreadPoolExecutor executorService,
                                AtomicInteger executingThreadCounter,
                                Set<Long> inspectedRoots) {
        Page rootPage = rootReference.root;
        long pos = rootPage.getPos();
        if (rootPage.isSaved()) {
            if (inspectedRoots != null && !inspectedRoots.add(pos)) {
                return;
            }
            collector.setMapId(meta.getId());
            collector.visit(pos, executorService, executingThreadCounter);
        }
        for (Cursor<String, String> c = new Cursor<>(rootPage, "root."); c.hasNext(); ) {
            String key = c.next();
            if (!key.startsWith("root.")) {
                break;
            }
            pos = DataUtils.parseHexLong(c.getValue());
            if (DataUtils.isPageSaved(pos)) {
                if (inspectedRoots == null || inspectedRoots.add(pos)) {
                    // to allow for something like "root.tmp.123" to be processed
                    int mapId = DataUtils.parseHexInt(key.substring(key.lastIndexOf('.') + 1));
                    collector.setMapId(mapId);
                    collector.visit(pos, executorService, executingThreadCounter);
                }
            }
        }
    }

    final class ChunkIdsCollector {

        /** really a set */
        private final ConcurrentHashMap<Integer, Integer> referencedChunks = new ConcurrentHashMap<>();
        private final ChunkIdsCollector parent;
        private       int               mapId;

        ChunkIdsCollector(int mapId) {
            this.parent = null;
            this.mapId = mapId;
        }

        private ChunkIdsCollector(ChunkIdsCollector parent) {
            this.parent = parent;
            this.mapId = parent.mapId;
        }

        public int getMapId() {
            return mapId;
        }

        public void setMapId(int mapId) {
            this.mapId = mapId;
        }

        public Set<Integer> getReferenced() {
            return new HashSet<>(referencedChunks.keySet());
        }

        /**
         * Visit a page on a chunk and collect ids for it and its children.
         *
         * @param page the page to visit
         * @param executorService the service to use when doing visit in parallel
         * @param executingThreadCounter number of threads currently active
         */
        public void visit(Page page, ThreadPoolExecutor executorService, AtomicInteger executingThreadCounter) {
            long pos = page.getPos();
            if (DataUtils.isPageSaved(pos)) {
                registerChunk(DataUtils.getPageChunkId(pos));
            }
            int count = page.map.getChildPageCount(page);
            if (count == 0) {
                return;
            }
            ChunkIdsCollector childCollector = DataUtils.isPageSaved(pos) && cacheChunkRef != null ?
                                                        new ChunkIdsCollector(this) : this;
            for (int i = 0; i < count; i++) {
                Page childPage = page.getChildPageIfLoaded(i);
                if (childPage != null) {
                    childCollector.visit(childPage, executorService, executingThreadCounter);
                } else {
                    childCollector.visit(page.getChildPagePos(i), executorService, executingThreadCounter);
                }
            }
            cacheCollectedChunkIds(pos, childCollector);
        }

        /**
         * Visit a page on a chunk and collect ids for it and its children.
         *
         * @param pos position of the page to visit
         * @param executorService the service to use when doing visit in parallel
         * @param executingThreadCounter number of threads currently active
         */
        public void visit(long pos, ThreadPoolExecutor executorService, AtomicInteger executingThreadCounter) {
            if (!DataUtils.isPageSaved(pos)) {
                return;
            }
            registerChunk(DataUtils.getPageChunkId(pos));
            if (DataUtils.getPageType(pos) == DataUtils.PAGE_TYPE_LEAF) {
                return;
            }
            int[] chunkIds;
            if (cacheChunkRef != null && (chunkIds = cacheChunkRef.get(pos)) != null) {
                // there is a cached set of chunk ids for this position
                for (int chunkId : chunkIds) {
                    registerChunk(chunkId);
                }
            } else {
                ChunkIdsCollector childCollector = cacheChunkRef != null ? new ChunkIdsCollector(this) : this;
                Page page;
                if (cache != null && (page = cache.get(pos)) != null) {
                    // there is a full page in cache, use it
                    childCollector.visit(page, executorService, executingThreadCounter);
                } else {
                    // page was not cached: read the data
                    ByteBuffer buff = readBufferForPage(pos, getMapId());
                    Page.readChildrenPositions(buff, pos, childCollector, executorService, executingThreadCounter);
                }
                cacheCollectedChunkIds(pos, childCollector);
            }
        }

        /**
         * Add chunk to list of referenced chunks.
         *
         * @param chunkId chunk id
         */
        void registerChunk(int chunkId) {
            if (referencedChunks.put(chunkId, 1) == null && parent != null) {
                parent.registerChunk(chunkId);
            }
        }

        private void cacheCollectedChunkIds(long pos, ChunkIdsCollector childCollector) {
            if (childCollector != this) {
                int[] chunkIds = new int[childCollector.referencedChunks.size()];
                int index = 0;
                for (Integer chunkId : childCollector.referencedChunks.keySet()) {
                    chunkIds[index++] = chunkId;
                }
                cacheChunkRef.put(pos, chunkIds, Constants.MEMORY_ARRAY + 4 * chunkIds.length);
            }
        }
    }

    /**
     * Get a buffer for writing. This caller must synchronize on the store
     * before calling the method and until after using the buffer.
     *
     * @return the buffer
     */
    private WriteBuffer getWriteBuffer() {
        WriteBuffer buff;
        if (writeBuffer != null) {
            buff = writeBuffer;
            buff.clear();
        } else {
            buff = new WriteBuffer();
        }
        return buff;
    }

    /**
     * Release a buffer for writing. This caller must synchronize on the store
     * before calling the method and until after using the buffer.
     *
     * @param buff the buffer than can be re-used
     */
    private void releaseWriteBuffer(WriteBuffer buff) {
        if (buff.capacity() <= 4 * 1024 * 1024) {
            writeBuffer = buff;
        }
    }

    private boolean canOverwriteChunk(Chunk c, long time) {
        if (retentionTime >= 0) {
            if (c.time + retentionTime > time) {
                return false;
            }
            if (c.unused == 0 || c.unused + retentionTime / 2 > time) {
                return false;
            }
        }
        return true;
    }

    private long getTimeSinceCreation() {
        return Math.max(0, getTimeAbsolute() - creationTime);
    }

    private long getTimeAbsolute() {
        long now = System.currentTimeMillis();
        if (lastTimeAbsolute != 0 && now < lastTimeAbsolute) {
            // time seems to have run backwards - this can happen
            // when the system time is adjusted, for example
            // on a leap second
            now = lastTimeAbsolute;
        } else {
            lastTimeAbsolute = now;
        }
        return now;
    }

    /**
     * Apply the freed space to the chunk metadata. The metadata is updated, but
     * completely free chunks are not removed from the set of chunks, and the
     * disk space is not yet marked as free.
     */
    private void applyFreedSpace() {
        while (true) {
            ArrayList<Chunk> modified = new ArrayList<>();
            synchronized (freedPageSpace) {
                for (Chunk f : freedPageSpace.values()) {
                    Chunk c = chunks.get(f.id);
                    if (c != null) { // skip if was already removed
                        c.maxLenLive += f.maxLenLive;
                        c.pageCountLive += f.pageCountLive;
                        if (c.pageCountLive < 0 && c.pageCountLive > -MARKED_FREE) {
                            // can happen after a rollback
                            c.pageCountLive = 0;
                        }
                        if (c.maxLenLive < 0 && c.maxLenLive > -MARKED_FREE) {
                            // can happen after a rollback
                            c.maxLenLive = 0;
                        }
                        modified.add(c);
                    }
                }
                freedPageSpace.clear();
            }
            if (modified.isEmpty()) {
                break;
            }
            for (Chunk c : modified) {
                meta.put(Chunk.getMetaKey(c.id), c.asString());
            }
            markMetaChanged();
        }
    }

    /**
     * Shrink the file if possible, and if at least a given percentage can be
     * saved.
     *
     * @param minPercent the minimum percentage to save
     */
    private void shrinkFileIfPossible(int minPercent) {
        if (fileStore.isReadOnly()) {
            return;
        }
        long end = getFileLengthInUse();
        long fileSize = fileStore.size();
        if (end >= fileSize) {
            return;
        }
        if (minPercent > 0 && fileSize - end < BLOCK_SIZE) {
            return;
        }
        int savedPercent = (int) (100 - (end * 100 / fileSize));
        if (savedPercent < minPercent) {
            return;
        }
        if (isOpenOrStopping()) {
            sync();
        }
        fileStore.truncate(end);
    }

    /**
     * Get the position right after the last used byte.
     *
     * @return the position
     */
    private long getFileLengthInUse() {
        long result = fileStore.getFileLengthInUse();
        assert result == measureFileLengthInUse() : result + " != " + measureFileLengthInUse();
        return result;
    }

    private long measureFileLengthInUse() {
        long size = 2;
        for (Chunk c : chunks.values()) {
            if (c.len != Integer.MAX_VALUE) {
                size = Math.max(size, c.block + c.len);
            }
        }
        return size * BLOCK_SIZE;
    }

    /**
     * Check whether there are any unsaved changes.
     *
     * @return if there are any changes
     */
    public boolean hasUnsavedChanges() {
        if (metaChanged) {
            return true;
        }
        for (MVMap<?, ?> m : maps.values()) {
            if (!m.isClosed()) {
                if(m.hasChangesSince(lastStoredVersion)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean hasUnsavedChangesInternal() {
        if (meta.hasChangesSince(lastStoredVersion)) {
            return true;
        }
        return hasUnsavedChanges();
    }

    private Chunk readChunkHeader(long block) {
        long p = block * BLOCK_SIZE;
        ByteBuffer buff = fileStore.readFully(p, Chunk.MAX_HEADER_LENGTH);
        return Chunk.readChunkHeader(buff, p);
    }

    /**
     * Compact the store by moving all live pages to new chunks.
     *
     * @return if anything was written
     */
    public boolean compactRewriteFully() {
        storeLock.lock();
        try {
            checkOpen();
            if (lastChunk == null) {
                // nothing to do
                return false;
            }
            for (MVMap<?, ?> m : maps.values()) {
                @SuppressWarnings("unchecked")
                MVMap<Object, Object> map = (MVMap<Object, Object>) m;
                Cursor<Object, Object> cursor = map.cursor(null);
                Page lastPage = null;
                while (cursor.hasNext()) {
                    cursor.next();
                    Page p = cursor.getPage();
                    if (p == lastPage) {
                        continue;
                    }
                    Object k = p.getKey(0);
                    Object v = p.getValue(0);
                    map.put(k, v);
                    lastPage = p;
                }
            }
            commit();
            return true;
        } finally {
            storeLock.unlock();
        }

    }

    /**
     * Compact by moving all chunks next to each other.
     */
    public void compactMoveChunks() {
        compactMoveChunks(100, Long.MAX_VALUE);
    }

    /**
     * Compact the store by moving all chunks next to each other, if there is
     * free space between chunks. This might temporarily increase the file size.
     * Chunks are overwritten irrespective of the current retention time. Before
     * overwriting chunks and before resizing the file, syncFile() is called.
     *
     * @param targetFillRate do nothing if the file store fill rate is higher
     *            than this
     * @param moveSize the number of bytes to move
     */
    public void compactMoveChunks(int targetFillRate, long moveSize) {
        storeLock.lock();
        try {
            checkOpen();
            if (lastChunk != null && reuseSpace) {
                int oldRetentionTime = retentionTime;
                boolean oldReuse = reuseSpace;
                try {
                    retentionTime = -1;
                    freeUnusedChunks(false);
                    if (fileStore.getFillRate() <= targetFillRate) {
                        long start = fileStore.getFirstFree() / BLOCK_SIZE;
                        ArrayList<Chunk> move = findChunksToMove(start, moveSize);
                        compactMoveChunks(move);
                    }
                } finally {
                    reuseSpace = oldReuse;
                    retentionTime = oldRetentionTime;
                }
            }
        } finally {
            storeLock.unlock();
        }
    }

    private ArrayList<Chunk> findChunksToMove(long startBlock, long moveSize) {
        ArrayList<Chunk> move = new ArrayList<>();
        for (Chunk c : chunks.values()) {
            if (c.block > startBlock) {
                move.add(c);
            }
        }
        // sort by block
        Collections.sort(move, new Comparator<Chunk>() {
            @Override
            public int compare(Chunk o1, Chunk o2) {
                return Long.signum(o1.block - o2.block);
            }
        });
        // find which is the last block to keep
        int count = 0;
        long size = 0;
        for (Chunk c : move) {
            long chunkSize = c.len * (long) BLOCK_SIZE;
            size += chunkSize;
            if (size > moveSize) {
                break;
            }
            count++;
        }
        // move the first block (so the first gap is moved),
        // and the one at the end (so the file shrinks)
        while (move.size() > count && move.size() > 1) {
            move.remove(1);
        }

        return move;
    }

    private void compactMoveChunks(ArrayList<Chunk> move) {
        for (Chunk c : move) {
            moveChunk(c, true);
        }

        // update the metadata (store at the end of the file)
        reuseSpace = false;
        commit();
        sync();

        Chunk chunk = this.lastChunk;

        // now re-use the empty space
        reuseSpace = true;
        for (Chunk c : move) {
            // ignore if already removed during the previous store operation
            if (chunks.containsKey(c.id)) {
                moveChunk(c, false);
            }
        }

        // update the metadata (within the file)
        commit();
        sync();
        if (chunks.containsKey(chunk.id)) {
            moveChunk(chunk, false);
            commit();
        }
        shrinkFileIfPossible(0);
        sync();
    }

    private void moveChunk(Chunk c, boolean toTheEnd) {
        WriteBuffer buff = getWriteBuffer();
        long start = c.block * BLOCK_SIZE;
        int length = c.len * BLOCK_SIZE;
        buff.limit(length);
        ByteBuffer readBuff = fileStore.readFully(start, length);
        Chunk.readChunkHeader(readBuff, start);
        int chunkHeaderLen = readBuff.position();
        buff.position(chunkHeaderLen);
        buff.put(readBuff);
        long pos = allocateFileSpace(length, toTheEnd);
        fileStore.free(start, length);
        c.block = pos / BLOCK_SIZE;
        c.next = 0;
        buff.position(0);
        c.writeChunkHeader(buff, chunkHeaderLen);
        buff.position(length - Chunk.FOOTER_LENGTH);
        buff.put(c.getFooterBytes());
        buff.position(0);
        write(pos, buff.getBuffer());
        releaseWriteBuffer(buff);
        meta.put(Chunk.getMetaKey(c.id), c.asString());
        markMetaChanged();
    }

    private long allocateFileSpace(int length, boolean atTheEnd) {
        long filePos;
        if (atTheEnd) {
            filePos = getFileLengthInUse();
            fileStore.markUsed(filePos, length);
        } else {
            filePos = fileStore.allocate(length);
        }
        return filePos;
    }

    /**
     * Force all stored changes to be written to the storage. The default
     * implementation calls FileChannel.force(true).
     */
    public void sync() {
        checkOpen();
        FileStore f = fileStore;
        if (f != null) {
            f.sync();
        }
    }

    /**
     * Try to increase the fill rate by re-writing partially full chunks. Chunks
     * with a low number of live items are re-written.
     * <p>
     * If the current fill rate is higher than the target fill rate, nothing is
     * done.
     * <p>
     * Please note this method will not necessarily reduce the file size, as
     * empty chunks are not overwritten.
     * <p>
     * Only data of open maps can be moved. For maps that are not open, the old
     * chunk is still referenced. Therefore, it is recommended to open all maps
     * before calling this method.
     *
     * @param targetFillRate the minimum percentage of live entries
     * @param write the minimum number of bytes to write
     * @return if a chunk was re-written
     */
    public boolean compact(int targetFillRate, int write) {
        if (!reuseSpace) {
            return false;
        }
        checkOpen();
        // We can't wait forever for the lock here,
        // because if called from the background thread,
        // it might go into deadlock with concurrent database closure
        // and attempt to stop this thread.
        try {
            if (!storeLock.isHeldByCurrentThread() &&
                    storeLock.tryLock(10, TimeUnit.MILLISECONDS)) {
                try {
                    ArrayList<Chunk> old = findOldChunks(targetFillRate, write);
                    if (old == null || old.isEmpty()) {
                        return false;
                    }
                    compactRewrite(old);
                    return true;
                } finally {
                    storeLock.unlock();
                }
            }
            return false;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the current fill rate (percentage of used space in the file). Unlike
     * the fill rate of the store, here we only account for chunk data; the fill
     * rate here is how much of the chunk data is live (still referenced). Young
     * chunks are considered live.
     *
     * @return the fill rate, in percent (100 is completely full)
     */
    public int getCurrentFillRate() {
        long maxLengthSum = 1;
        long maxLengthLiveSum = 1;
        long time = getTimeSinceCreation();
        for (Chunk c : chunks.values()) {
            maxLengthSum += c.maxLen;
            if (c.time + retentionTime > time) {
                // young chunks (we don't optimize those):
                // assume if they are fully live
                // so that we don't try to optimize yet
                // until they get old
                maxLengthLiveSum += c.maxLen;
            } else {
                maxLengthLiveSum += c.maxLenLive;
            }
        }
        // the fill rate of all chunks combined
        if (maxLengthSum <= 0) {
            // avoid division by 0
            maxLengthSum = 1;
        }
        int fillRate = (int) (100 * maxLengthLiveSum / maxLengthSum);
        return fillRate;
    }

    private ArrayList<Chunk> findOldChunks(int targetFillRate, int write) {
        if (lastChunk == null) {
            // nothing to do
            return null;
        }
        long time = getTimeSinceCreation();
        int fillRate = getCurrentFillRate();
        if (fillRate >= targetFillRate) {
            return null;
        }

        // the 'old' list contains the chunks we want to free up
        ArrayList<Chunk> old = new ArrayList<>();
        Chunk last = chunks.get(lastChunk.id);
        for (Chunk c : chunks.values()) {
            // only look at chunk older than the retention time
            // (it's possible to compact chunks earlier, but right
            // now we don't do that)
            if (c.time + retentionTime <= time) {
                long age = last.version - c.version + 1;
                c.collectPriority = (int) (c.getFillRate() * 1000 / Math.max(1,age));
                old.add(c);
            }
        }
        if (old.isEmpty()) {
            return null;
        }

        // sort the list, so the first entry should be collected first
        Collections.sort(old, new Comparator<Chunk>() {
            @Override
            public int compare(Chunk o1, Chunk o2) {
                int comp = Integer.compare(o1.collectPriority, o2.collectPriority);
                if (comp == 0) {
                    comp = Long.compare(o1.maxLenLive, o2.maxLenLive);
                }
                return comp;
            }
        });
        // find out up to were in the old list we need to move
        long written = 0;
        int chunkCount = 0;
        Chunk move = null;
        for (Chunk c : old) {
            if (move != null) {
                if (c.collectPriority > 0 && written > write) {
                    break;
                }
            }
            written += c.maxLenLive;
            chunkCount++;
            move = c;
        }
        if (chunkCount < 1) {
            return null;
        }
        // remove the chunks we want to keep from this list
        boolean remove = false;
        for (Iterator<Chunk> it = old.iterator(); it.hasNext();) {
            Chunk c = it.next();
            if (move == c) {
                remove = true;
            } else if (remove) {
                it.remove();
            }
        }
        return old;
    }

    private void compactRewrite(Iterable<Chunk> old) {
        HashSet<Integer> set = new HashSet<>();
        for (Chunk c : old) {
            set.add(c.id);
        }
        for (MVMap<?, ?> m : maps.values()) {
            @SuppressWarnings("unchecked")
            MVMap<Object, Object> map = (MVMap<Object, Object>) m;
            if (!map.isClosed()) {
                map.rewrite(set);
            }
        }
        meta.rewrite(set);
        freeUnusedChunks(false);
        commit();
    }

    /**
     * Read a page.
     *
     * @param map the map
     * @param pos the page position
     * @return the page
     */
    Page readPage(MVMap<?, ?> map, long pos) {
        if (!DataUtils.isPageSaved(pos)) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT, "Position 0");
        }
        Page p = cache == null ? null : cache.get(pos);
        if (p == null) {
            ByteBuffer buff = readBufferForPage(pos, map.getId());
            p = Page.read(buff, pos, map);
            cachePage(p);
        }
        return p;
    }

    /**
     * Remove a page.
     *
     * @param pos the position of the page
     * @param memory the memory usage
     */
    void removePage(long pos, int memory) {
        // we need to keep temporary pages,
        // to support reading old versions and rollback
        if (!DataUtils.isPageSaved(pos)) {
            // the page was not yet stored:
            // just using "unsavedMemory -= memory" could result in negative
            // values, because in some cases a page is allocated, but never
            // stored, so we need to use max
            unsavedMemory = Math.max(0, unsavedMemory - memory);
            return;
        }

        int chunkId = DataUtils.getPageChunkId(pos);
        // synchronize, because pages could be freed concurrently
        synchronized (freedPageSpace) {
            Chunk chunk = freedPageSpace.get(chunkId);
            if (chunk == null) {
                chunk = new Chunk(chunkId);
                freedPageSpace.put(chunkId, chunk);
            }
            chunk.maxLenLive -= DataUtils.getPageMaxLength(pos);
            chunk.pageCountLive -= 1;
        }
    }

    Compressor getCompressorFast() {
        if (compressorFast == null) {
            compressorFast = new CompressLZF();
        }
        return compressorFast;
    }

    Compressor getCompressorHigh() {
        if (compressorHigh == null) {
            compressorHigh = new CompressDeflate();
        }
        return compressorHigh;
    }

    int getCompressionLevel() {
        return compressionLevel;
    }

    public int getPageSplitSize() {
        return pageSplitSize;
    }

    public int getKeysPerPage() {
        return keysPerPage;
    }

    public long getMaxPageSize() {
        return cache == null ? Long.MAX_VALUE : cache.getMaxItemSize() >> 4;
    }

    public boolean getReuseSpace() {
        return reuseSpace;
    }

    /**
     * Whether empty space in the file should be re-used. If enabled, old data
     * is overwritten (default). If disabled, writes are appended at the end of
     * the file.
     * <p>
     * This setting is specially useful for online backup. To create an online
     * backup, disable this setting, then copy the file (starting at the
     * beginning of the file). In this case, concurrent backup and write
     * operations are possible (obviously the backup process needs to be faster
     * than the write operations).
     *
     * @param reuseSpace the new value
     */
    public void setReuseSpace(boolean reuseSpace) {
        this.reuseSpace = reuseSpace;
    }

    public int getRetentionTime() {
        return retentionTime;
    }

    /**
     * How long to retain old, persisted chunks, in milliseconds. Chunks that
     * are older may be overwritten once they contain no live data.
     * <p>
     * The default value is 45000 (45 seconds) when using the default file
     * store. It is assumed that a file system and hard disk will flush all
     * write buffers within this time. Using a lower value might be dangerous,
     * unless the file system and hard disk flush the buffers earlier. To
     * manually flush the buffers, use
     * <code>MVStore.getFile().force(true)</code>, however please note that
     * according to various tests this does not always work as expected
     * depending on the operating system and hardware.
     * <p>
     * The retention time needs to be long enough to allow reading old chunks
     * while traversing over the entries of a map.
     * <p>
     * This setting is not persisted.
     *
     * @param ms how many milliseconds to retain old chunks (0 to overwrite them
     *            as early as possible)
     */
    public void setRetentionTime(int ms) {
        this.retentionTime = ms;
    }

    /**
     * How many versions to retain for in-memory stores. If not set, 5 old
     * versions are retained.
     *
     * @param count the number of versions to keep
     */
    public void setVersionsToKeep(int count) {
        this.versionsToKeep = count;
    }

    /**
     * Get the oldest version to retain in memory (for in-memory stores).
     *
     * @return the version
     */
    public long getVersionsToKeep() {
        return versionsToKeep;
    }

    /**
     * Get the oldest version to retain in memory, which is the manually set
     * retain version, or the current store version (whatever is older).
     *
     * @return the version
     */
    public long getOldestVersionToKeep() {
        long v = oldestVersionToKeep.get();
        if (fileStore == null) {
            v = Math.max(v - versionsToKeep + 1, INITIAL_VERSION);
            return v;
        }

        long storeVersion = currentStoreVersion;
        if (storeVersion != INITIAL_VERSION && storeVersion < v) {
            v = storeVersion;
        }
        return v;
    }

    private void setOldestVersionToKeep(long oldestVersionToKeep) {
        boolean success;
        do {
            long current = this.oldestVersionToKeep.get();
            // Oldest version may only advance, never goes back
            success = oldestVersionToKeep <= current ||
                        this.oldestVersionToKeep.compareAndSet(current, oldestVersionToKeep);
        } while (!success);
    }

    /**
     * Check whether all data can be read from this version. This requires that
     * all chunks referenced by this version are still available (not
     * overwritten).
     *
     * @param version the version
     * @return true if all data can be read
     */
    private boolean isKnownVersion(long version) {
        if (version > currentVersion || version < 0) {
            return false;
        }
        if (version == currentVersion || chunks.isEmpty()) {
            // no stored data
            return true;
        }
        // need to check if a chunk for this version exists
        Chunk c = getChunkForVersion(version);
        if (c == null) {
            return false;
        }
        // also, all chunks referenced by this version
        // need to be available in the file
        MVMap<String, String> oldMeta = getMetaMap(version);
        if (oldMeta == null) {
            return false;
        }
        try {
            for (Iterator<String> it = oldMeta.keyIterator("chunk.");
                    it.hasNext();) {
                String chunkKey = it.next();
                if (!chunkKey.startsWith("chunk.")) {
                    break;
                }
                if (!meta.containsKey(chunkKey)) {
                    String s = oldMeta.get(chunkKey);
                    Chunk c2 = Chunk.fromString(s);
                    Chunk test = readChunkHeaderAndFooter(c2.block);
                    if (test == null || test.id != c2.id) {
                        return false;
                    }
                }
            }
        } catch (IllegalStateException e) {
            // the chunk missing where the metadata is stored
            return false;
        }
        return true;
    }

    /**
     * Increment the number of unsaved pages.
     *
     * @param memory the memory usage of the page
     */
    public void registerUnsavedPage(int memory) {
        unsavedMemory += memory;
        int newValue = unsavedMemory;
        if (newValue > autoCommitMemory && autoCommitMemory > 0) {
            saveNeeded = true;
        }
    }

    public boolean isSaveNeeded() {
        return saveNeeded;
    }

    /**
     * This method is called before writing to a map.
     *
     * @param map the map
     */
    void beforeWrite(MVMap<?, ?> map) {
        if (saveNeeded && fileStore != null && isOpenOrStopping()) {
            saveNeeded = false;
            // check again, because it could have been written by now
            if (unsavedMemory > autoCommitMemory && autoCommitMemory > 0) {
                // if unsaved memory creation rate is to high,
                // some back pressure need to be applied
                // to slow things down and avoid OOME
                if (3 * unsavedMemory > 4 * autoCommitMemory) {
                    commit();
                } else {
                    tryCommit();
                }
            }
        }
    }

    /**
     * Get the store version. The store version is usually used to upgrade the
     * structure of the store after upgrading the application. Initially the
     * store version is 0, until it is changed.
     *
     * @return the store version
     */
    public int getStoreVersion() {
        checkOpen();
        String x = meta.get("setting.storeVersion");
        return x == null ? 0 : DataUtils.parseHexInt(x);
    }

    /**
     * Update the store version.
     *
     * @param version the new store version
     */
    public void setStoreVersion(int version) {
        storeLock.lock();
        try {
            checkOpen();
            markMetaChanged();
            meta.put("setting.storeVersion", Integer.toHexString(version));
        } finally {
            storeLock.unlock();
        }
    }

    /**
     * Revert to the beginning of the current version, reverting all uncommitted
     * changes.
     */
    public void rollback() {
        rollbackTo(currentVersion);
    }

    /**
     * Revert to the beginning of the given version. All later changes (stored
     * or not) are forgotten. All maps that were created later are closed. A
     * rollback to a version before the last stored version is immediately
     * persisted. Rollback to version 0 means all data is removed.
     *
     * @param version the version to revert to
     */
    public void rollbackTo(long version) {
        storeLock.lock();
        try {
            checkOpen();
            if (version == 0) {
                // special case: remove all data
                for (MVMap<?, ?> m : maps.values()) {
                    m.close();
                }
                meta.setInitialRoot(meta.createEmptyLeaf(), INITIAL_VERSION);

                chunks.clear();
                if (fileStore != null) {
                    fileStore.clear();
                }
                maps.clear();
                lastChunk = null;
                synchronized (freedPageSpace) {
                    freedPageSpace.clear();
                }
                versions.clear();
                currentVersion = version;
                setWriteVersion(version);
                metaChanged = false;
                lastStoredVersion = INITIAL_VERSION;
                return;
            }
            DataUtils.checkArgument(
                    isKnownVersion(version),
                    "Unknown version {0}", version);
            for (MVMap<?, ?> m : maps.values()) {
                m.rollbackTo(version);
            }

            TxCounter txCounter;
            while ((txCounter = versions.peekLast()) != null && txCounter.version >= version) {
                versions.removeLast();
            }
            currentTxCounter = new TxCounter(version);

            meta.rollbackTo(version);
            metaChanged = false;
            boolean loadFromFile = false;
            // find out which chunks to remove,
            // and which is the newest chunk to keep
            // (the chunk list can have gaps)
            ArrayList<Integer> remove = new ArrayList<>();
            Chunk keep = null;
            for (Chunk c : chunks.values()) {
                if (c.version > version) {
                    remove.add(c.id);
                } else if (keep == null || keep.id < c.id) {
                    keep = c;
                }
            }
            if (!remove.isEmpty()) {
                // remove the youngest first, so we don't create gaps
                // (in case we remove many chunks)
                Collections.sort(remove, Collections.reverseOrder());
                loadFromFile = true;
                for (int id : remove) {
                    Chunk c = chunks.remove(id);
                    long start = c.block * BLOCK_SIZE;
                    int length = c.len * BLOCK_SIZE;
                    fileStore.free(start, length);
                    assert fileStore.getFileLengthInUse() == measureFileLengthInUse() :
                            fileStore.getFileLengthInUse() + " != " + measureFileLengthInUse();
                    // overwrite the chunk,
                    // so it is not be used later on
                    WriteBuffer buff = getWriteBuffer();
                    buff.limit(length);
                    // buff.clear() does not set the data
                    Arrays.fill(buff.getBuffer().array(), (byte) 0);
                    write(start, buff.getBuffer());
                    releaseWriteBuffer(buff);
                    // only really needed if we remove many chunks, when writes are
                    // re-ordered - but we do it always, because rollback is not
                    // performance critical
                    sync();
                }
                lastChunk = keep;
                writeStoreHeader();
                readStoreHeader();
            }
            for (MVMap<?, ?> m : new ArrayList<>(maps.values())) {
                int id = m.getId();
                if (m.getCreateVersion() >= version) {
                    m.close();
                    maps.remove(id);
                } else {
                    if (loadFromFile) {
                        m.setRootPos(getRootPos(meta, id), version);
                    } else {
                        m.rollbackRoot(version);
                    }
                }
            }
            currentVersion = version;
            if (lastStoredVersion == INITIAL_VERSION) {
                lastStoredVersion = currentVersion - 1;
            }
        } finally {
            storeLock.unlock();
        }
    }

    private static long getRootPos(MVMap<String, String> map, int mapId) {
        String root = map.get(MVMap.getMapRootKey(mapId));
        return root == null ? 0 : DataUtils.parseHexLong(root);
    }

    /**
     * Get the current version of the data. When a new store is created, the
     * version is 0.
     *
     * @return the version
     */
    public long getCurrentVersion() {
        return currentVersion;
    }

    public long getLastStoredVersion() {
        return lastStoredVersion;
    }

    /**
     * Get the file store.
     *
     * @return the file store
     */
    public FileStore getFileStore() {
        return fileStore;
    }

    /**
     * Get the store header. This data is for informational purposes only. The
     * data is subject to change in future versions. The data should not be
     * modified (doing so may corrupt the store).
     *
     * @return the store header
     */
    public Map<String, Object> getStoreHeader() {
        return storeHeader;
    }

    private void checkOpen() {
        if (!isOpenOrStopping()) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_CLOSED,
                    "This store is closed", panicException);
        }
    }

    /**
     * Rename a map.
     *
     * @param map the map
     * @param newName the new name
     */
    public void renameMap(MVMap<?, ?> map, String newName) {
        checkOpen();
        DataUtils.checkArgument(map != meta,
                "Renaming the meta map is not allowed");
        int id = map.getId();
        String oldName = getMapName(id);
        if (oldName != null && !oldName.equals(newName)) {
            String idHexStr = Integer.toHexString(id);
            // at first create a new name as an "alias"
            String existingIdHexStr = meta.putIfAbsent("name." + newName, idHexStr);
            // we need to cope with the case of previously unfinished rename
            DataUtils.checkArgument(
                    existingIdHexStr == null || existingIdHexStr.equals(idHexStr),
                    "A map named {0} already exists", newName);
            // switch roles of a new and old names - old one is an alias now
            meta.put(MVMap.getMapKey(id), map.asString(newName));
            // get rid of the old name completely
            meta.remove("name." + oldName);
            markMetaChanged();
        }
    }

    /**
     * Remove a map. Please note rolling back this operation does not restore
     * the data; if you need this ability, use Map.clear().
     *
     * @param map the map to remove
     */
    public void removeMap(MVMap<?, ?> map) {
        removeMap(map, true);
    }

    /**
     * Remove a map.
     *
     * @param map the map to remove
     * @param delayed whether to delay deleting the metadata
     */
    public void removeMap(MVMap<?, ?> map, boolean delayed) {
        storeLock.lock();
        try {
            checkOpen();
            DataUtils.checkArgument(map != meta,
                    "Removing the meta map is not allowed");
            map.close();
            RootReference rootReference = map.getRoot();
            updateCounter += rootReference.updateCounter;
            updateAttemptCounter += rootReference.updateAttemptCounter;

            int id = map.getId();
            String name = getMapName(id);
            removeMap(name, id, delayed);
        } finally {
            storeLock.unlock();
        }
    }

    private void removeMap(String name, int id, boolean delayed) {
        if (meta.remove(MVMap.getMapKey(id)) != null) {
            markMetaChanged();
        }
        if (meta.remove("name." + name) != null) {
            markMetaChanged();
        }
        if (!delayed) {
            if (meta.remove(MVMap.getMapRootKey(id)) != null) {
                markMetaChanged();
            }
            maps.remove(id);
        }
    }

    /**
     * Remove map by name.
     *
     * @param name the map name
     */
    public void removeMap(String name) {
        int id = getMapId(name);
        if(id > 0) {
            removeMap(name, id, false);
        }
    }

    /**
     * Get the name of the given map.
     *
     * @param id the map id
     * @return the name, or null if not found
     */
    public String getMapName(int id) {
        checkOpen();
        String m = meta.get(MVMap.getMapKey(id));
        return m == null ? null : DataUtils.getMapName(m);
    }

    private int getMapId(String name) {
        String m = meta.get("name." + name);
        return m == null ? -1 : DataUtils.parseHexInt(m);
    }

    /**
     * Commit and save all changes, if there are any, and compact the store if
     * needed.
     */
    void writeInBackground() {
        try {
            if (!isOpenOrStopping()) {
                return;
            }

            // could also commit when there are many unsaved pages,
            // but according to a test it doesn't really help

            long time = getTimeSinceCreation();
            if (time <= lastCommitTime + autoCommitDelay) {
                return;
            }
            tryCommit();
            if (autoCompactFillRate > 0) {
                // whether there were file read or write operations since
                // the last time
                boolean fileOps;
                long fileOpCount = fileStore.getWriteCount() + fileStore.getReadCount();
                if (autoCompactLastFileOpCount != fileOpCount) {
                    fileOps = true;
                } else {
                    fileOps = false;
                }
                // use a lower fill rate if there were any file operations
                int targetFillRate = fileOps ? autoCompactFillRate / 3 : autoCompactFillRate;
                compact(targetFillRate, autoCommitMemory);
                autoCompactLastFileOpCount = fileStore.getWriteCount() + fileStore.getReadCount();
            }
        } catch (Throwable e) {
            handleException(e);
        }
    }

    private void handleException(Throwable ex) {
        if (backgroundExceptionHandler != null) {
            try {
                backgroundExceptionHandler.uncaughtException(Thread.currentThread(), ex);
            } catch(Throwable ignore) {
                if (ex != ignore) { // OOME may be the same
                    ex.addSuppressed(ignore);
                }
            }
        }
    }

    /**
     * Set the read cache size in MB.
     *
     * @param mb the cache size in MB.
     */
    public void setCacheSize(int mb) {
        final long bytes = (long) mb * 1024 * 1024;
        if (cache != null) {
            cache.setMaxMemory(bytes);
            cache.clear();
        }
        if (cacheChunkRef != null) {
            cacheChunkRef.setMaxMemory(bytes / 4);
            cacheChunkRef.clear();
        }
    }

    private boolean isOpen() {
        return state == STATE_OPEN;
    }

    /**
     * Determine that store is open, or wait for it to be closed (by other thread)
     * @return true if store is open, false otherwise
     */
    public boolean isClosed() {
        if (isOpen()) {
            return false;
        }
        storeLock.lock();
        try {
            assert state == STATE_CLOSED;
            return true;
        } finally {
            storeLock.unlock();
        }
    }

    private boolean isOpenOrStopping() {
        return state <= STATE_STOPPING;
    }

    private void stopBackgroundThread(boolean waitForIt) {
        // Loop here is not strictly necessary, except for case of a spurious failure,
        // which should not happen with non-weak flavour of CAS operation,
        // but I've seen it, so just to be safe...
        BackgroundWriterThread t;
        while ((t = backgroundWriterThread.get()) != null) {
            if (backgroundWriterThread.compareAndSet(t, null)) {
                // if called from within the thread itself - can not join
                if (t != Thread.currentThread()) {
                    synchronized (t.sync) {
                        t.sync.notifyAll();
                    }

                    if (waitForIt) {
                        try {
                            t.join();
                        } catch (Exception e) {
                            // ignore
                        }
                    }
                }
                break;
            }
        }
    }

    /**
     * Set the maximum delay in milliseconds to auto-commit changes.
     * <p>
     * To disable auto-commit, set the value to 0. In this case, changes are
     * only committed when explicitly calling commit.
     * <p>
     * The default is 1000, meaning all changes are committed after at most one
     * second.
     *
     * @param millis the maximum delay
     */
    public void setAutoCommitDelay(int millis) {
        if (autoCommitDelay == millis) {
            return;
        }
        autoCommitDelay = millis;
        if (fileStore == null || fileStore.isReadOnly()) {
            return;
        }
        stopBackgroundThread(true);
        // start the background thread if needed
        if (millis > 0 && isOpen()) {
            int sleep = Math.max(1, millis / 10);
            BackgroundWriterThread t =
                    new BackgroundWriterThread(this, sleep,
                            fileStore.toString());
            if (backgroundWriterThread.compareAndSet(null, t)) {
                t.start();
            }
        }
    }

    boolean isBackgroundThread() {
        return Thread.currentThread() == backgroundWriterThread.get();
    }

    /**
     * Get the auto-commit delay.
     *
     * @return the delay in milliseconds, or 0 if auto-commit is disabled.
     */
    public int getAutoCommitDelay() {
        return autoCommitDelay;
    }

    /**
     * Get the maximum memory (in bytes) used for unsaved pages. If this number
     * is exceeded, unsaved changes are stored to disk.
     *
     * @return the memory in bytes
     */
    public int getAutoCommitMemory() {
        return autoCommitMemory;
    }

    /**
     * Get the estimated memory (in bytes) of unsaved data. If the value exceeds
     * the auto-commit memory, the changes are committed.
     * <p>
     * The returned value is an estimation only.
     *
     * @return the memory in bytes
     */
    public int getUnsavedMemory() {
        return unsavedMemory;
    }

    /**
     * Put the page in the cache.
     * @param page the page
     */
    void cachePage(Page page) {
        if (cache != null) {
            cache.put(page.getPos(), page, page.getMemory());
        }
    }

    /**
     * Get the amount of memory used for caching, in MB.
     * Note that this does not include the page chunk references cache, which is
     * 25% of the size of the page cache.
     *
     * @return the amount of memory used for caching
     */
    public int getCacheSizeUsed() {
        if (cache == null) {
            return 0;
        }
        return (int) (cache.getUsedMemory() >> 20);
    }

    /**
     * Get the maximum cache size, in MB.
     * Note that this does not include the page chunk references cache, which is
     * 25% of the size of the page cache.
     *
     * @return the cache size
     */
    public int getCacheSize() {
        if (cache == null) {
            return 0;
        }
        return (int) (cache.getMaxMemory() >> 20);
    }

    /**
     * Get the cache.
     *
     * @return the cache
     */
    public CacheLongKeyLIRS<Page> getCache() {
        return cache;
    }

    /**
     * Whether the store is read-only.
     *
     * @return true if it is
     */
    public boolean isReadOnly() {
        return fileStore != null && fileStore.isReadOnly();
    }

    public double getUpdateFailureRatio() {
        long updateCounter = this.updateCounter;
        long updateAttemptCounter = this.updateAttemptCounter;
        RootReference rootReference = meta.getRoot();
        updateCounter += rootReference.updateCounter;
        updateAttemptCounter += rootReference.updateAttemptCounter;
        for (MVMap<?, ?> map : maps.values()) {
            RootReference root = map.getRoot();
            updateCounter += root.updateCounter;
            updateAttemptCounter += root.updateAttemptCounter;
        }
        return updateAttemptCounter == 0 ? 0 : 1 - ((double)updateCounter / updateAttemptCounter);
    }

    /**
     * Register opened operation (transaction).
     * This would increment usage counter for the current version.
     * This version (and all after it) should not be dropped until all
     * transactions involved are closed and usage counter goes to zero.
     * @return TxCounter to be decremented when operation finishes (transaction closed).
     */
    public TxCounter registerVersionUsage() {
        TxCounter txCounter;
        while(true) {
            txCounter = currentTxCounter;
            if(txCounter.counter.getAndIncrement() >= 0) {
                break;
            }
            // The only way for counter to be negative
            // if it was retrieved right before onVersionChange()
            // and now onVersionChange() is done.
            // This version is eligible for reclamation now
            // and should not be used here, so restore count
            // not to upset accounting and try again with a new
            // version (currentTxCounter should have changed).
            assert txCounter != currentTxCounter : txCounter;
            txCounter.counter.decrementAndGet();
        }
        return txCounter;
    }

    /**
     * De-register (close) completed operation (transaction).
     * This will decrement usage counter for the corresponding version.
     * If counter reaches zero, that version (and all unused after it)
     * can be dropped immediately.
     *
     * @param txCounter to be decremented, obtained from registerVersionUsage()
     */
    public void deregisterVersionUsage(TxCounter txCounter) {
        if(txCounter != null) {
            if(txCounter.counter.decrementAndGet() <= 0) {
                if (!storeLock.isHeldByCurrentThread() && storeLock.tryLock()) {
                    try {
                        dropUnusedVersions();
                    } finally {
                        storeLock.unlock();
                    }
                }
            }
        }
    }

    private void onVersionChange(long version) {
        TxCounter txCounter = this.currentTxCounter;
        assert txCounter.counter.get() >= 0;
        versions.add(txCounter);
        currentTxCounter = new TxCounter(version);
        txCounter.counter.decrementAndGet();
        dropUnusedVersions();
    }

    private void dropUnusedVersions() {
        TxCounter txCounter;
        while ((txCounter = versions.peek()) != null
                && txCounter.counter.get() < 0) {
            versions.poll();
        }
        setOldestVersionToKeep(txCounter != null ? txCounter.version : currentTxCounter.version);
    }

    /**
     * Class TxCounter is a simple data structure to hold version of the store
     * along with the counter of open transactions,
     * which are still operating on this version.
     */
    public static final class TxCounter {
        /**
         * Version of a store, this TxCounter is related to
         */
        public final long version;

        /**
         * Counter of outstanding operation on this version of a store
         */
        public final AtomicInteger counter = new AtomicInteger();

        TxCounter(long version) {
            this.version = version;
        }

        @Override
        public String toString() {
            return "v=" + version + " / cnt=" + counter;
        }
    }

    /**
     * A background writer thread to automatically store changes from time to
     * time.
     */
    private static class BackgroundWriterThread extends Thread {

        public final Object sync = new Object();
        private final MVStore store;
        private final int sleep;

        BackgroundWriterThread(MVStore store, int sleep, String fileStoreName) {
            super("MVStore background writer " + fileStoreName);
            this.store = store;
            this.sleep = sleep;
            setDaemon(true);
        }

        @Override
        public void run() {
            while (store.isBackgroundThread()) {
                synchronized (sync) {
                    try {
                        sync.wait(sleep);
                    } catch (InterruptedException ignore) {
                    }
                }
                if (!store.isBackgroundThread()) {
                    break;
                }
                store.writeInBackground();
            }
        }
    }

    /**
     * A builder for an MVStore.
     */
    public static final class Builder {

        private final HashMap<String, Object> config;

        private Builder(HashMap<String, Object> config) {
            this.config = config;
        }

        /**
         * Creates new instance of MVStore.Builder.
         */
        public Builder() {
            config = new HashMap<>();
        }

        private Builder set(String key, Object value) {
            config.put(key, value);
            return this;
        }

        /**
         * Disable auto-commit, by setting the auto-commit delay and auto-commit
         * buffer size to 0.
         *
         * @return this
         */
        public Builder autoCommitDisabled() {
            // we have a separate config option so that
            // no thread is started if the write delay is 0
            // (if we only had a setter in the MVStore,
            // the thread would need to be started in any case)
            //set("autoCommitBufferSize", 0);
            return set("autoCommitDelay", 0);
        }

        /**
         * Set the size of the write buffer, in KB disk space (for file-based
         * stores). Unless auto-commit is disabled, changes are automatically
         * saved if there are more than this amount of changes.
         * <p>
         * The default is 1024 KB.
         * <p>
         * When the value is set to 0 or lower, data is not automatically
         * stored.
         *
         * @param kb the write buffer size, in kilobytes
         * @return this
         */
        public Builder autoCommitBufferSize(int kb) {
            return set("autoCommitBufferSize", kb);
        }

        /**
         * Set the auto-compact target fill rate. If the average fill rate (the
         * percentage of the storage space that contains active data) of the
         * chunks is lower, then the chunks with a low fill rate are re-written.
         * Also, if the percentage of empty space between chunks is higher than
         * this value, then chunks at the end of the file are moved. Compaction
         * stops if the target fill rate is reached.
         * <p>
         * The default value is 40 (40%). The value 0 disables auto-compacting.
         * <p>
         *
         * @param percent the target fill rate
         * @return this
         */
        public Builder autoCompactFillRate(int percent) {
            return set("autoCompactFillRate", percent);
        }

        /**
         * Use the following file name. If the file does not exist, it is
         * automatically created. The parent directory already must exist.
         *
         * @param fileName the file name
         * @return this
         */
        public Builder fileName(String fileName) {
            return set("fileName", fileName);
        }

        /**
         * Encrypt / decrypt the file using the given password. This method has
         * no effect for in-memory stores. The password is passed as a
         * char array so that it can be cleared as soon as possible. Please note
         * there is still a small risk that password stays in memory (due to
         * Java garbage collection). Also, the hashed encryption key is kept in
         * memory as long as the file is open.
         *
         * @param password the password
         * @return this
         */
        public Builder encryptionKey(char[] password) {
            return set("encryptionKey", password);
        }

        /**
         * Open the file in read-only mode. In this case, a shared lock will be
         * acquired to ensure the file is not concurrently opened in write mode.
         * <p>
         * If this option is not used, the file is locked exclusively.
         * <p>
         * Please note a store may only be opened once in every JVM (no matter
         * whether it is opened in read-only or read-write mode), because each
         * file may be locked only once in a process.
         *
         * @return this
         */
        public Builder readOnly() {
            return set("readOnly", 1);
        }

        /**
         * Set the read cache size in MB. The default is 16 MB.
         *
         * @param mb the cache size in megabytes
         * @return this
         */
        public Builder cacheSize(int mb) {
            return set("cacheSize", mb);
        }

        /**
         * Set the read cache concurrency. The default is 16, meaning 16
         * segments are used.
         *
         * @param concurrency the cache concurrency
         * @return this
         */
        public Builder cacheConcurrency(int concurrency) {
            return set("cacheConcurrency", concurrency);
        }

        /**
         * Compress data before writing using the LZF algorithm. This will save
         * about 50% of the disk space, but will slow down read and write
         * operations slightly.
         * <p>
         * This setting only affects writes; it is not necessary to enable
         * compression when reading, even if compression was enabled when
         * writing.
         *
         * @return this
         */
        public Builder compress() {
            return set("compress", 1);
        }

        /**
         * Compress data before writing using the Deflate algorithm. This will
         * save more disk space, but will slow down read and write operations
         * quite a bit.
         * <p>
         * This setting only affects writes; it is not necessary to enable
         * compression when reading, even if compression was enabled when
         * writing.
         *
         * @return this
         */
        public Builder compressHigh() {
            return set("compress", 2);
        }

        /**
         * Set the amount of memory a page should contain at most, in bytes,
         * before it is split. The default is 16 KB for persistent stores and 4
         * KB for in-memory stores. This is not a limit in the page size, as
         * pages with one entry can get larger. It is just the point where pages
         * that contain more than one entry are split.
         *
         * @param pageSplitSize the page size
         * @return this
         */
        public Builder pageSplitSize(int pageSplitSize) {
            return set("pageSplitSize", pageSplitSize);
        }

        /**
         * Set the listener to be used for exceptions that occur when writing in
         * the background thread.
         *
         * @param exceptionHandler the handler
         * @return this
         */
        public Builder backgroundExceptionHandler(
                Thread.UncaughtExceptionHandler exceptionHandler) {
            return set("backgroundExceptionHandler", exceptionHandler);
        }

        /**
         * Use the provided file store instead of the default one.
         * <p>
         * File stores passed in this way need to be open. They are not closed
         * when closing the store.
         * <p>
         * Please note that any kind of store (including an off-heap store) is
         * considered a "persistence", while an "in-memory store" means objects
         * are not persisted and fully kept in the JVM heap.
         *
         * @param store the file store
         * @return this
         */
        public Builder fileStore(FileStore store) {
            return set("fileStore", store);
        }

        /**
         * Open the store.
         *
         * @return the opened store
         */
        public MVStore open() {
            return new MVStore(config);
        }

        @Override
        public String toString() {
            return DataUtils.appendMap(new StringBuilder(), config).toString();
        }

        /**
         * Read the configuration from a string.
         *
         * @param s the string representation
         * @return the builder
         */
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public static Builder fromString(String s) {
            // Cast from HashMap<String, String> to HashMap<String, Object> is safe
            return new Builder((HashMap) DataUtils.parseMap(s));
        }
    }
}
