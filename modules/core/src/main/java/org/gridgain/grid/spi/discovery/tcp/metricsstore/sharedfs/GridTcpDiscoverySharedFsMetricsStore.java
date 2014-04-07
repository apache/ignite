/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */


package org.gridgain.grid.spi.discovery.tcp.metricsstore.sharedfs;

import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.discovery.*;
import org.gridgain.grid.spi.discovery.tcp.metricsstore.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jdk8.backport.*;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

/**
 * Shared filesystem-based metrics store.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * There are no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * <ul>
 *     <li>Path (see {@link #setPath(String)}).</li>
 *     <li>Metrics expire time (see {@link #setMetricsExpireTime(int)}).</li>
 * </ul>
 * <p>
 * If {@link #getPath()} is not provided, then {@link #DFLT_PATH} will be used and
 * only local nodes will discover each other. To enable metrics store working over
 * network you must provide a path to a shared directory explicitly.
 * <p>
 * The directory will contain files with serialized metrics named like the following
 * 94816A59-EB51-44EE-BB67-8B24B9C10A09.
 */
public class GridTcpDiscoverySharedFsMetricsStore extends GridTcpDiscoveryMetricsStoreAdapter {
    /** Default path for local testing only. */
    public static final String DFLT_PATH = "work/disco/metrics-store";

    /** Grid logger. */
    @GridLoggerResource
    private GridLogger log;

    /** File-system path. */
    private String path = DFLT_PATH;

    /** Folder to keep items in. */
    @GridToStringExclude
    private File folder;

    /** Warning guard. */
    @GridToStringExclude
    private final AtomicBoolean warnGuard = new AtomicBoolean();

    /** Init guard. */
    @GridToStringExclude
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /** Init latch. */
    @GridToStringExclude
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /** Locks to synchronize within single JVM. */
    private final ConcurrentMap<UUID, Lock> locks = new ConcurrentHashMap8<>();

    /**
     * Gets path.
     *
     * @return Shared path.
     */
    public String getPath() {
        return path;
    }

    /**
     * Sets path.
     *
     * @param path Shared path.
     */
    @GridSpiConfiguration(optional = true)
    public void setPath(String path) {
        this.path = path;
    }

    /**
     * Initializes folder to work with.
     *
     * @return Folder.
     * @throws GridSpiException If failed.
     */
    private File initFolder() throws GridSpiException {
        if (initGuard.compareAndSet(false, true)) {
            if (path == null)
                throw new GridSpiException("Shared file system path is null " +
                    "(you must configure it via setPath(..) configuration property)");

            if (path.equals(DFLT_PATH) && warnGuard.compareAndSet(false, true))
                U.warn(log, "Default local computer-only share is used by metrics store.");

            try {
                URL folderUrl = U.resolveGridGainUrl(path);

                if (folderUrl == null)
                    throw new GridSpiException("Failed to resolve path: " + path);

                File tmp;

                try {
                    tmp = new File(folderUrl.toURI());
                }
                catch (URISyntaxException e) {
                    throw new GridSpiException("Failed to resolve path: " + path, e);
                }

                if (!tmp.isDirectory())
                    throw new GridSpiException("Failed to initialize shared file system path " +
                        "(path must point to folder): " + path);

                if (!tmp.canRead() || !tmp.canWrite())
                    throw new GridSpiException("Failed to initialize shared file system path " +
                        "(path must be readable and writable): " + path);

                folder = tmp;
            }
            finally {
                initLatch.countDown();
            }
        }
        else {
            try {
                U.await(initLatch);
            }
            catch (GridInterruptedException e) {
                throw new GridSpiException("Thread has been interrupted.", e);
            }

            if (folder == null)
                throw new GridSpiException("Metrics store has not been properly initialized.");
        }

        return folder;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"TooBroadScope"})
    @Override public void updateLocalMetrics(UUID locNodeId, GridNodeMetrics metrics) throws GridSpiException {
        assert locNodeId != null;
        assert metrics != null;

        initFolder();

        FileLock lock = null;

        RandomAccessFile file = null;

        Lock jvmLock = getLock(locNodeId);

        jvmLock.lock();

        try {
            file = new RandomAccessFile(new File(folder, locNodeId.toString()), "rw");

            FileChannel ch = file.getChannel();

            lock = ch.lock();

            byte[] buf = new byte[GridDiscoveryMetricsHelper.METRICS_SIZE];

            GridDiscoveryMetricsHelper.serialize(buf, 0, metrics);

            ch.write(ByteBuffer.wrap(buf));
        }
        catch (IOException e) {
            U.error(log, "Failed to update local node metrics [locNodeId=" + locNodeId +
                ", metrics=" + metrics + ']', e);
        }
        finally {
            if (lock != null)
                try {
                    lock.release();
                }
                catch (Throwable e) {
                    U.error(log, "Failed to release lock when updating metrics for local node: " + locNodeId, e);
                }

            U.closeQuiet(file);

            jvmLock.unlock();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"TooBroadScope"})
    @Override protected Map<UUID, GridNodeMetrics> metrics0(Collection<UUID> nodeIds) throws GridSpiException {
        assert !F.isEmpty(nodeIds);

        initFolder();

        Map<UUID, GridNodeMetrics> res = new HashMap<>();

        for (UUID id : nodeIds) {
            FileLock lock = null;

            RandomAccessFile file = null;

            Lock jvmLock = getLock(id);

            jvmLock.lock();

            try {
                file = new RandomAccessFile(new File(folder, id.toString()), "r");

                FileChannel ch = file.getChannel();

                lock = ch.lock(0, Long.MAX_VALUE, true);

                byte[] buf = new byte[GridDiscoveryMetricsHelper.METRICS_SIZE];

                ch.read(ByteBuffer.wrap(buf));

                res.put(id, GridDiscoveryMetricsHelper.deserialize(buf, 0));
            }
            catch (FileNotFoundException e) {
                if (log.isDebugEnabled())
                    log.debug("Provided node id has not been found in the store [id=" + id +
                        ", err=" + e.getMessage() + ']');
            }
            catch (IOException e) {
                U.error(log, "Failed to read metrics for node: " + id, e);
            }
            finally {
                if (lock != null)
                    try {
                        lock.release();
                    }
                    catch (Throwable e) {
                        U.error(log, "Failed to release lock when reading metrics for node: " + id, e);
                    }

                U.closeQuiet(file);

                jvmLock.unlock();
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> allNodeIds() throws GridSpiException {
        initFolder();

        Collection<UUID> ids = new LinkedList<>();

        for (File file : folder.listFiles())
            if (file.isFile())
                try {
                    UUID id = UUID.fromString(file.getName());

                    ids.add(id);
                }
                catch (IllegalArgumentException ignored) {
                    U.warn(log, "Failed to parse UUID from file name: " + file.getName());
                }

        return ids;
    }

    /** {@inheritDoc} */
    @Override public void removeMetrics0(Collection<UUID> nodeIds) throws GridSpiException {
        assert !F.isEmpty(nodeIds);

        initFolder();

        for (UUID id : nodeIds) {
            Lock jvmLock = getLock(id);

            jvmLock.lock();

            try {
                new File(folder, id.toString()).delete();
            }
            catch (Exception e) {
                U.error(log, "Failed to remove metrics for node: " + id, e);
            }
            finally {
                jvmLock.unlock();
            }
        }
    }

    /**
     * @param nodeId Node ID.
     * @return Lock.
     */
    private Lock getLock(UUID nodeId) {
        assert nodeId != null;

        Lock lock = new ReentrantLock();

        Lock old = locks.putIfAbsent(nodeId, lock);

        return old == null ? lock : old;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTcpDiscoverySharedFsMetricsStore.class, this);
    }
}
