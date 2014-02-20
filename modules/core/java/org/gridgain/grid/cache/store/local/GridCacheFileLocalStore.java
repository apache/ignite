// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.store.local;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import static org.gridgain.grid.cache.store.local.GridCacheFileLocalStoreWriteMode.*;

/**
 * Local file based {@link GridCacheStore} implementation. Internally it uses compact offheap map for key hashes and
 * pointers to append only update log files. When log file become too sparse (contain too many entries that have already
 * been removed logically) it will be compacted by background process.
 * <h2 class="header">Configuration</h2>
 * Sections below describe mandatory and optional configuration settings as well
 * as providing example using Java and Spring XML.
 * <h3>Mandatory</h3>
 * There are no mandatory configuration parameters.
 *<h3>Optional</h3>
 * <ul>
 *     <li>Write mode (see {@link #setWriteMode(GridCacheFileLocalStoreWriteMode)}</li>
 *     <li>Write delay (see {@link #setWriteDelay(long)}</li>
 *     <li>Write buffer size  (see {@link #setWriteBufferSize(int)}</li>
 *     <li>Fsync delay (see {@link #setFsyncDelay(long)}</li>
 *     <li>Read buffer size (see {@link #setReadBufferSize(int)}</li>
 *     <li>Compaction buffer size (see {@link #setCompactBufferSize(int)}</li>
 *     <li>Minimum compaction file size (see {@link #setMinCompactSize(long)}</li>
 *     <li>Maximum allowed file sparcity (see {@link #setMaxSparsity(float)}</li>
 *     <li>Sparcity check frequency (see {@link #setSparcityCheckFrequency(long)}</li>
 *     <li>Root directory path (see {@link #setRootPath(String)}</li>
 *     <li>Calculate checksum flag (see {@link #setChecksum(boolean)}</li>
 *     <li>Initial map capacity (see {@link #setMapCapacity(int)}</li>
 *     <li>Number of map segments (see {@link #setMapSegments(int)}</li>
 * </ul>
 * <h2 class="header">Java Example</h2>
 * <pre name="code" class="java">
 *     ...
 *     GridCacheStore&lt;String, GridTuple2&lt;String, ?&gt;&gt; store = new GridCacheFileLocalStore&lt;String, String&gt;();
 *     ...
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * <pre name="code" class="xml">
 *     ...
 *     &lt;bean id=&quot;cache.local.store&quot;
 *         class=&quot;org.gridgain.grid.cache.store.local.GridCacheFileLocalStore&quot;&gt;
 *     &lt;/bean&gt;
 *     ...
 * </pre>
 * <p>
 * <img src="http://www.gridgain.com/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 *
 * @author @java.author
 * @version @java.version
 */
@SuppressWarnings("PackageVisibleField")
@GridCacheLocalStore
public class GridCacheFileLocalStore<K, V> implements GridCacheStore<K, GridBiTuple<V, ?>>, GridLifecycleAware {
    /** Default value for fsync delay in milliseconds. */
    public static final int DFLT_FSYNC_DELAY = 500; // 0.5 sec.

    /** Default value for write delay in microseconds. */
    public static final int DFLT_WRITE_DELAY = 5000; // 5ms

    /** Default write mode. */
    public static final GridCacheFileLocalStoreWriteMode DFLT_COMMIT_MODE = ASYNC_BUFFERED;

    /** Default write buffer size. */
    public static final int DFLT_WRITE_BUFFER_SIZE = 128 * 1024;

    /** Default read buffer size. */
    public static final int DFLT_READ_BUFFER_SIZE = 1024;

    /** Default sparcity check frequency. */
    public static final long DFLT_SPARCITY_CHECK_FREQUENCY = 500;

    /** Default compact buffer size. */
    public static final int DFLT_COMPACT_BUFFER_SIZE = 256 * 1024;

    /** Default map capacity. */
    public static final int DFLT_MAP_CAPACITY = 16 * 1024;

    /** Default number of map segments. */
    public static final int DFLT_MAP_SEGMENTS = 32;

    /** Default maximum sparsity. */
    public static final float DFLT_MAX_SPARSITY = 0.35f;

    /** Default minimum compaction size. */
    public static final long DFLT_MIN_COMPACT_SIZE = 100 * 1024 * 1024;

    /** Default root path for file storage. */
    public static final String DFLT_ROOT_PATH = "work/localstore";

    /** */
    private static final String TX_DELTA_META_KEY = "store-tx-delta";

    /** */
    private final GridAtomicInitializer<GridCacheFileLocalStoreFileManager> space = new GridAtomicInitializer<>();

    /** */
    @GridMarshallerResource
    protected GridMarshaller marsh;

    /** */
    @GridNameResource
    protected String gridName;

    /** */
    @GridCacheNameResource
    protected String cacheName;

    /** */
    @GridLoggerResource
    protected GridLogger log;

    /** */
    int writeBufSize = DFLT_WRITE_BUFFER_SIZE;

    /** */
    int readBufSize = DFLT_READ_BUFFER_SIZE;

    /** */
    int compactBufSize = DFLT_COMPACT_BUFFER_SIZE;

    /** */
    long sparcityCheckFreq = DFLT_SPARCITY_CHECK_FREQUENCY;

    /** Microseconds. */
    long fsyncDelay = DFLT_FSYNC_DELAY * 1000;

    /** Microseconds. */
    long writeDelay = DFLT_WRITE_DELAY;

    /** */
    GridCacheFileLocalStoreWriteMode writeMode = DFLT_COMMIT_MODE;

    /** */
    float maxSparsity = DFLT_MAX_SPARSITY;

    /** */
    long minCompactSize = DFLT_MIN_COMPACT_SIZE;

    /** */
    boolean checksum;

    /** */
    int mapCap = DFLT_MAP_CAPACITY;

    /** */
    int mapSegments = DFLT_MAP_SEGMENTS;

    /** */
    private String rootPath = DFLT_ROOT_PATH;

    /** */
    Path root;

    /**
     * Sets write buffer size in bytes. Usually it is preferable to have it large enough to accommodate multiple
     * key-value entries to flush them to the disk at once. When the key-value entry is large itself, then
     * it does not make much sense though.
     *
     * @param writeBufSize Buffer size.
     */
    public void setWriteBufferSize(int writeBufSize) {
        A.ensure(writeBufSize >= 40, "writeBufSize >= 40");

        this.writeBufSize = writeBufSize;
    }

    /**
     * Sets read buffer size. This buffer will be allocated for each read, it is preferable to have it not that large.
     *
     * @param readBufSize Buffer size.
     */
    public void setReadBufferSize(int readBufSize) {
        A.ensure(readBufSize >= 40, "readBufSize >= 40");

        this.readBufSize = readBufSize;
    }

    /**
     * Sets compact buffer size. This buffer will be used for compaction. It is preferable to have it larger than
     * write buffer {@link #setWriteBufferSize(int)} size.
     *
     * @param compactBufSize Buffer size.
     */
    public void setCompactBufferSize(int compactBufSize) {
        A.ensure(compactBufSize >= 40, "compactBufSize >= 40");

        this.compactBufSize = compactBufSize;
    }

    /**
     * Sets frequency to check sparcity of files for compaction.
     *
     * @see #setMaxSparsity(float)
     *
     * @param sparcityCheckFreq Sparcity check frequency in milliseconds.
     */
    public void setSparcityCheckFrequency(long sparcityCheckFreq) {
        A.ensure(sparcityCheckFreq >= 20, "sparcityCheckFreq >= 20");

        this.sparcityCheckFreq = sparcityCheckFreq;
    }

    /**
     * Sets maximum delay in milliseconds after which fsync will be issued after a write to a file.
     * If the value is {@code 0} then fsync will be issued immediately.
     *
     * @param fsyncDelay Fsync delay in milliseconds.
     */
    public void setFsyncDelay(long fsyncDelay) {
        A.ensure(fsyncDelay >= 0, "fsyncDelay >= 0");

        this.fsyncDelay = fsyncDelay * 1000;
    }

    /**
     * Sets write delay after which write buffer can be flushed to the disk.
     * Makes sense only for {@link GridCacheFileLocalStoreWriteMode#SYNC_BUFFERED} and
     * {@link GridCacheFileLocalStoreWriteMode#ASYNC_BUFFERED} write modes.
     *
     * @param writeDelay Write delay in microseconds.
     */
    public void setWriteDelay(long writeDelay) {
        A.ensure(writeDelay >= 0, "writeDelay >= 0");

        this.writeDelay = writeDelay;
    }

    /**
     * Sets write mode.
     *
     * @see GridCacheFileLocalStoreWriteMode
     *
     * @param writeMode Write mode.
     */
    public void setWriteMode(GridCacheFileLocalStoreWriteMode writeMode) {
        A.notNull(writeMode, "writeMode");

        this.writeMode = writeMode;
    }

    /**
     * Sets maximum allowed file sparcity above which compaction has to start. Calculated as waste size in the file to
     * the whole file size ratio. Must be between {@code 0} and {@code 1}.
     *
     * @param maxSparsity Maximum sparcity.
     */
    public void setMaxSparsity(float maxSparsity) {
        A.ensure(maxSparsity > 0 && maxSparsity < 1, "maxSparsity > 0 && maxSparsity < 1");

        this.maxSparsity = maxSparsity;
    }

    /**
     * Sets minimum file size for compaction. File can be compacted only when its size is greater than specified value
     * in bytes.
     *
     * @param minCompactSize Minimum compact size.
     */
    public void setMinCompactSize(long minCompactSize) {
        A.ensure(minCompactSize > 0, "minCompactSize > 0");

        this.minCompactSize = minCompactSize;
    }

    /**
     * Enables or disables checksum calculation for entries on disk.
     *
     * @param checksum Checksum.
     */
    public void setChecksum(boolean checksum) {
        this.checksum = checksum;
    }

    /**
     * Sets initial internal hash map capacity. Must be power of 2.
     *
     * @param mapCap Initial map capacity.
     */
    public void setMapCapacity(int mapCap) {
        A.ensure(U.isPow2(mapCap), "mapCap must be power of 2");

        this.mapCap = mapCap;
    }

    /**
     * Sets number of map segments. The more segments map has, the greater parallelism level.
     * Must be power of 2.
     *
     * @param mapSegments Number of map segments.
     */
    public void setMapSegments(int mapSegments) {
        A.ensure(U.isPow2(mapSegments), "mapSegments must be power of 2");

        this.mapSegments = mapSegments;
    }

    /**
     * Sets root directory path for storage.
     *
     * @param rootPath Root path.
     */
    public void setRootPath(String rootPath) {
        A.notNull(rootPath, "rootPath");

        this.rootPath = rootPath;
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stop() throws GridException {
        GridCacheFileLocalStoreFileManager s = fileManager(false);

        if (s != null)
            s.stop();
    }

    /**
     * Number of live entries in the store for the given cache name.
     * For testing.
     *
     * @return Number of entries.
     * @throws GridException If failed.
     */
    int size() throws GridException {
        return fileManager(true).size();
    }

    /**
     * For testing.
     *
     * @throws GridException If failed.
     */
    void clear() throws GridException {
        assert fileManager(false) == null;

        try {
            File dir = resolveDirectory().toFile();

            X.println("Clearing " + dir + " | " + dir.exists() + " | " + U.delete(dir) + " | " + dir.exists());
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridBiTuple<V, ?> load(@Nullable GridCacheTx tx, K key)
        throws GridException {
        Map<K, GridBiTuple<V, ?>> delta = delta(tx, false);

        if (!F.isEmpty(delta) && delta.containsKey(key))
            return delta.get(key);

        return fileManager(true).load(key);
    }

    /** {@inheritDoc} */
    @Override public void loadAll(GridBiInClosure<K, GridBiTuple<V, ?>> clo,
        @Nullable Object... args) throws GridException {
        fileManager(true).loadAll(clo);
    }

    /** {@inheritDoc} */
    @Override public void loadAll(@Nullable GridCacheTx tx, @Nullable Collection<? extends K> keys,
        final GridBiInClosure<K, GridBiTuple<V, ?>> c) throws GridException {
        if (F.isEmpty(keys))
            return;

        if (keys.size() == 1) {
            K key = keys.iterator().next();

            c.apply(key, load(tx, key));

            return;
        }

        final Map<K, GridBiTuple<V, ?>> delta = delta(tx, false);

        GridCacheFileLocalStoreFileManager space = fileManager(true);

        space.loadAll(keys, F.isEmpty(delta) ? c : new CI2<K, GridBiTuple<V, ?>>() {
            @Override public void apply(K key, GridBiTuple<V, ?> val) {
                if (delta.containsKey(key))
                    val = delta.get(key);

                if (val != null)
                    c.apply(key, val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void put(@Nullable GridCacheTx tx, K key,
        @Nullable GridBiTuple<V, ?> val) throws GridException {
        Map<K, GridBiTuple<V, ?>> delta = delta(tx, true);

        if (delta == null) {
            assert tx == null;

            fileManager(true).update(key, val);
        }
        else
            delta.put(key, val);
    }

    /** {@inheritDoc} */
    @Override public void putAll(@Nullable GridCacheTx tx,
        @Nullable Map<? extends K, ? extends GridBiTuple<V, ?>> map) throws GridException {
        Map<K, GridBiTuple<V, ?>> delta = delta(tx, true);

        if (delta == null) {
            assert tx == null;

            fileManager(true).updateAll(map.entrySet(), null);
        }
        else
            delta.putAll(map);
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable GridCacheTx tx, K key) throws GridException {
        Map<K, GridBiTuple<V, ?>> delta = delta(tx, true);

        if (delta == null) {
            assert tx == null;

            fileManager(true).update(key, null);
        }
        else
            delta.put(key, null);
    }

    /** {@inheritDoc} */
    @Override public void removeAll(@Nullable GridCacheTx tx, @Nullable Collection<? extends K> keys)
        throws GridException {
        Map<K, GridBiTuple<V, ?>> delta = delta(tx, true);

        if (delta == null) {
            assert tx == null;

            fileManager(true).removeAll(keys);
        }
        else {
            for (K key : keys)
                delta.put(key, null);
        }
    }

    /** {@inheritDoc} */
    @Override public void txEnd(GridCacheTx tx, boolean commit) throws GridException {
        Map<K, GridBiTuple<V, ?>> delta = tx.meta(TX_DELTA_META_KEY);

        if (!F.isEmpty(delta) && commit) {
            GridCacheFileLocalStoreFileManager s = fileManager(true);

            s.updateAll(delta.entrySet(), tx.xid());
        }
    }

    /**
     * @param tx Transaction.
     * @param createIfNone Create delta if none exists.
     * @return Delta.
     */
    @Nullable private Map<K, GridBiTuple<V, ?>> delta(@Nullable GridCacheTx tx, boolean createIfNone) {
        if (tx == null)
            return null;

        Map<K, GridBiTuple<V, ?>> delta = tx.meta(TX_DELTA_META_KEY);

        if (delta == null && createIfNone) {
            delta = new HashMap<>();

            Map<K, GridBiTuple<V, ?>> old = tx.addMeta(TX_DELTA_META_KEY, delta);

            assert old == null: "Concurrent access to single tx data.";
        }

        return delta;
    }

    /**
     * @param init Initialize if does not exists.
     * @return Space.
     * @throws GridException If failed.
     */
    @Nullable GridCacheFileLocalStoreFileManager fileManager(boolean init) throws GridException {
        GridAtomicInitializer<GridCacheFileLocalStoreFileManager> holder = space;

        if (holder.succeeded())
            return holder.result();

        if (!init)
            return null;

        return holder.init(new COX<GridCacheFileLocalStoreFileManager>() {
            @Override public GridCacheFileLocalStoreFileManager applyx() throws GridException {
                try {
                    return new GridCacheFileLocalStoreFileManager(GridCacheFileLocalStore.this, cacheName, resolveDirectory());
                }
                catch (IOException e) {
                    throw new GridException(e);
                }
            }
        });
    }

    /**
     * @return Directory path.
     * @throws IOException If failed.
     */
    private Path resolveDirectory() throws IOException {
        return root().resolve(U.maskName(gridName)).resolve(U.maskName(consistentId())).resolve(U.maskName(cacheName));
    }

    /**
     * @return Consistent ID.
     */
    String consistentId() {
        return G.grid(gridName).localNode().consistentId().toString();
    }

    /**
     * @return Database root directory.
     * @throws IOException If failed.
     */
    Path root() throws IOException {
        if (root == null) {
            File dir = new File(rootPath);

            if (dir.isAbsolute())
                root = dir.toPath();
            else if (!F.isEmpty(U.getGridGainHome()))
                root = new File(U.getGridGainHome(), rootPath).toPath();
            else
                throw new IOException("Failed to resolve database directory path: " + rootPath);
        }

        return root;
    }
}
