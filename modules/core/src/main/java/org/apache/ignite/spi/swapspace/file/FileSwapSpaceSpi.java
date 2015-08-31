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

package org.apache.ignite.spi.swapspace.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.GridAtomicInitializer;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiCloseableIterator;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.IgniteSpiThread;
import org.apache.ignite.spi.swapspace.SwapContext;
import org.apache.ignite.spi.swapspace.SwapKey;
import org.apache.ignite.spi.swapspace.SwapSpaceSpi;
import org.apache.ignite.spi.swapspace.SwapSpaceSpiListener;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.events.EventType.EVT_SWAP_SPACE_CLEARED;
import static org.apache.ignite.events.EventType.EVT_SWAP_SPACE_DATA_READ;
import static org.apache.ignite.events.EventType.EVT_SWAP_SPACE_DATA_REMOVED;
import static org.apache.ignite.events.EventType.EVT_SWAP_SPACE_DATA_STORED;

/**
 * File-based swap space SPI implementation which holds keys in memory. This SPI is used by default.
 * It is intended for use in cases when value size is bigger than {@code 100} bytes, otherwise it will not
 * have any positive effect.
 * <p>
 * <b>NOTE: This SPI does not support swap eviction currently, manual removes needed to reduce disk space
 * consumption.</b>
 * <p>
 * Every space has a name and when used in combination with in-memory data grid name and local node ID,
 * space name represents the actual cache name associated with this swap space. Default name is {@code null}
 * which is represented by {@link #DFLT_SPACE_NAME}.
 *
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This SPI has no mandatory configuration parameters.
 * <h2 class="header">Optional SPI configuration.</h2>
 * <ul>
 *     <li>Base directory path (see {@link #setBaseDirectory(String)}).</li>
 *     <li>Maximum sparsity (see {@link #setMaximumSparsity(float)}).</li>
 *     <li>Write buffer size in bytes (see {@link #setWriteBufferSize(int)}).</li>
 *     <li>Max write queue size in bytes (see {@link #setMaxWriteQueueSize(int)}).</li>
 *     <li>Read stripes number. (see {@link #setReadStripesNumber(int)}).</li>
 * </ul>
 *
 * <h2 class="header">Java Example</h2>
 * FileSwapSpaceSpi is configured by default and should be explicitly configured
 * only if some SPI configuration parameters need to be overridden.
 * <pre name="code" class="java">
 * FileSwapSpaceSpi spi = new FileSwapSpaceSpi();
 *
 * // Configure root folder path.
 * spi.setBaseDirectory("/path/to/swap/folder");
 *
 * IgniteConfiguration cfg = new IgniteConfiguration();
 *
 * // Override default swap space SPI.
 * cfg.setSwapSpaceSpi(spi);
 *
 * // Starts grid.
 * G.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * FileSwapSpaceSpi can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id=&quot;grid.cfg&quot; class=&quot;org.apache.ignite.configuration.IgniteConfiguration&quot; scope=&quot;singleton&quot;&gt;
 *     ...
 *     &lt;property name=&quot;swapSpaceSpi&quot;&gt;
 *         &lt;bean class=&quot;org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi&quot;&gt;
 *             &lt;property name=&quot;baseDirectory&quot; value=&quot;/path/to/swap/folder&quot;/&gt;
 *         &lt;/bean&gt;
 *     &lt;/property&gt;
 *     ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://ignite.apache.org/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 * @see org.apache.ignite.spi.swapspace.SwapSpaceSpi
 */
@IgniteSpiMultipleInstancesSupport(true)
@SuppressWarnings({"PackageVisibleInnerClass", "PackageVisibleField"})
public class FileSwapSpaceSpi extends IgniteSpiAdapter implements SwapSpaceSpi, FileSwapSpaceSpiMBean {
    /**
     * Default base directory. Note that this path is relative to {@code IGNITE_HOME/work} folder
     * if {@code IGNITE_HOME} system or environment variable specified, otherwise it is relative to
     * {@code work} folder under system {@code java.io.tmpdir} folder.
     *
     * @see org.apache.ignite.configuration.IgniteConfiguration#getWorkDirectory()
     */
    public static final String DFLT_BASE_DIR = "swapspace";

    /** Default maximum sparsity. */
    public static final float DFLT_MAX_SPARSITY = 0.5f;

    /** Default write buffer size in bytes. */
    public static final int DFLT_BUF_SIZE = 64 * 1024;

    /** Default write queue size in bytes. */
    public static final int DFLT_QUE_SIZE = 1024 * 1024;

    /** Name for {@code null} space. */
    public static final String DFLT_SPACE_NAME = "gg-dflt-space";

    /** Spaces. */
    private final ConcurrentMap<String, Space> spaces = new ConcurrentHashMap<>();

    /** Base directory. */
    private String baseDir = DFLT_BASE_DIR;

    /** Maximum sparsity. */
    private float maxSparsity = DFLT_MAX_SPARSITY;

    /** Eviction listener. */
    private volatile SwapSpaceSpiListener evictLsnr;

    /** Directory. */
    private File dir;

    /** Write buffer size. */
    private int writeBufSize = DFLT_BUF_SIZE;

    /** Max write queue size in bytes. */
    private int maxWriteQueSize = DFLT_QUE_SIZE;

    /** Read stripes number. */
    private int readStripesNum = -1;

    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override public String getBaseDirectory() {
        return baseDir;
    }

    /**
     * Sets base directory.
     *
     * @param baseDir Base directory.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setBaseDirectory(String baseDir) {
        this.baseDir = baseDir;
    }

    /** {@inheritDoc} */
    @Override public float getMaximumSparsity() {
        return maxSparsity;
    }

    /**
     * Sets maximum sparsity. This property defines maximum acceptable wasted file space to whole file size ratio.
     * When this ratio becomes higher than specified number compacting thread starts working.
     *
     * @param maxSparsity Maximum sparsity. Must be between 0 and 1, default is {@link #DFLT_MAX_SPARSITY}.
     */
    public void setMaximumSparsity(float maxSparsity) {
        this.maxSparsity = maxSparsity;
    }

    /** {@inheritDoc} */
    @Override public int getWriteBufferSize() {
        return writeBufSize;
    }

    /**
     * Sets write buffer size in bytes. Write to disk occurs only when this buffer is full. Default is
     * {@link #DFLT_BUF_SIZE}.
     *
     * @param writeBufSize Write buffer size in bytes.
     */
    public void setWriteBufferSize(int writeBufSize) {
        this.writeBufSize = writeBufSize;
    }

    /** {@inheritDoc} */
    @Override public int getMaxWriteQueueSize() {
        return maxWriteQueSize;
    }

    /**
     * Sets max write queue size in bytes. If there are more values are waiting for being written to disk then specified
     * size, SPI will block on {@link #store(String, org.apache.ignite.spi.swapspace.SwapKey, byte[], org.apache.ignite.spi.swapspace.SwapContext)} operation. Default is
     * {@link #DFLT_QUE_SIZE}.
     *
     * @param maxWriteQueSize Max write queue size in bytes.
     */
    public void setMaxWriteQueueSize(int maxWriteQueSize) {
        this.maxWriteQueSize = maxWriteQueSize;
    }

    /** {@inheritDoc} */
    @Override public int getReadStripesNumber() {
        return readStripesNum;
    }

    /**
     * Sets read stripe size. Defines number of file channels to be used concurrently. Default is equal to number of
     * CPU cores available to this JVM.
     *
     * @param readStripesNum Read stripe number.
     */
    public void setReadStripesNumber(int readStripesNum) {
        A.ensure(readStripesNum == -1 || (readStripesNum & (readStripesNum - 1)) == 0,
            "readStripesNum must be positive and power of two");

        this.readStripesNum = readStripesNum;
    }

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String gridName) throws IgniteSpiException {
        assertParameter(!F.isEmpty(baseDir), "!F.isEmpty(baseDir)");
        assertParameter(maxSparsity >= 0 && maxSparsity < 1, "maxSparsity >= 0 && maxSparsity < 1");
        assertParameter(readStripesNum == -1 || (readStripesNum & (readStripesNum - 1)) == 0,
            "readStripesNum must be positive and power of two.");

        if (readStripesNum == -1) {
            // User has not configured the number.
            int readStripesNum0 = 1;
            int cpuCnt = Runtime.getRuntime().availableProcessors();

            while (readStripesNum0 <= cpuCnt)
                readStripesNum0 <<= 1;

            if (readStripesNum0 > cpuCnt)
                readStripesNum0 >>= 1;

            assert readStripesNum0 > 0 && (readStripesNum0 & readStripesNum0 - 1) == 0;

            readStripesNum = readStripesNum0;
        }

        startStopwatch();

        registerMBean(gridName, this, FileSwapSpaceSpiMBean.class);

        String path = baseDir + File.separator + gridName + File.separator + ignite.configuration().getNodeId();

        try {
            dir = U.resolveWorkDirectory(path, true);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException(e);
        }

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        unregisterMBean();

        for (Space space : spaces.values()) {
            space.initialize();

            try {
                space.stop();
            }
            catch (IgniteInterruptedCheckedException e) {
                U.error(log, "Interrupted.", e);
            }
        }

        if (dir != null && dir.exists() && !U.delete(dir))
            U.warn(log, "Failed to delete swap directory: " + dir.getAbsolutePath());

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override public void clear(@Nullable String spaceName) throws IgniteSpiException {
        Space space = space(spaceName, false);

        if (space == null)
            return;

        space.clear();

        notifyListener(EVT_SWAP_SPACE_CLEARED, spaceName);
    }

    /** {@inheritDoc} */
    @Override public long size(@Nullable String spaceName) throws IgniteSpiException {
        Space space = space(spaceName, false);

        if (space == null)
            return 0;

        return space.size();
    }

    /** {@inheritDoc} */
    @Override public long count(@Nullable String spaceName) throws IgniteSpiException {
        Space space = space(spaceName, false);

        if (space == null)
            return 0;

        return space.count();
    }

    /** {@inheritDoc} */
    @Override public long count(@Nullable String spaceName, Set<Integer> parts) throws IgniteSpiException {
        Space space = space(spaceName, false);

        if (space == null)
            return 0;

        return space.count(parts);
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] read(@Nullable String spaceName, SwapKey key, SwapContext ctx)
        throws IgniteSpiException {
        assert key != null;
        assert ctx != null;

        Space space = space(spaceName, false);

        if (space == null)
            return null;

        byte[] val = space.read(key);

        notifyListener(EVT_SWAP_SPACE_DATA_READ, spaceName);

        return val;
    }

    /** {@inheritDoc} */
    @Override public Map<SwapKey, byte[]> readAll(@Nullable String spaceName, Iterable<SwapKey> keys,
        SwapContext ctx) throws IgniteSpiException {
        assert keys != null;
        assert ctx != null;

        Space space = space(spaceName, false);

        if (space == null)
            return Collections.emptyMap();

        Map<SwapKey, byte[]> res = new HashMap<>();

        for (SwapKey key : keys) {
            if (key != null) {
                byte[] val = space.read(key);

                if (val != null)
                    res.put(key, val);

                notifyListener(EVT_SWAP_SPACE_DATA_READ, spaceName);
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable String spaceName, SwapKey key, @Nullable IgniteInClosure<byte[]> c,
        SwapContext ctx) throws IgniteSpiException {
        assert key != null;
        assert ctx != null;

        Space space = space(spaceName, false);

        byte[] val = space == null ? null : space.remove(key, c != null);

        if (c != null)
            c.apply(val);

        if (space != null)
             notifyListener(EVT_SWAP_SPACE_DATA_REMOVED, spaceName);
    }

    /** {@inheritDoc} */
    @Override public void removeAll(@Nullable String spaceName, Collection<SwapKey> keys,
        @Nullable IgniteBiInClosure<SwapKey, byte[]> c, SwapContext ctx) throws IgniteSpiException {
        assert keys != null;
        assert ctx != null;

        Space space = space(spaceName, false);

        if (space == null)
            return;

        for (SwapKey key : keys) {
            if (key != null) {
                byte[] val = space.remove(key, c != null);

                if (c != null)
                    c.apply(key, val);

                notifyListener(EVT_SWAP_SPACE_DATA_REMOVED, spaceName);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void store(@Nullable String spaceName, SwapKey key, @Nullable byte[] val,
        SwapContext ctx) throws IgniteSpiException {
        assert key != null;
        assert ctx != null;

        Space space = space(spaceName, true);

        assert space != null;

        space.store(key, val);

        notifyListener(EVT_SWAP_SPACE_DATA_STORED, spaceName);
    }

    /** {@inheritDoc} */
    @Override public void storeAll(@Nullable String spaceName, Map<SwapKey, byte[]> pairs,
        SwapContext ctx) throws IgniteSpiException {
        assert pairs != null;
        assert ctx != null;

        Space space = space(spaceName, true);

        assert space != null;

        for (Map.Entry<SwapKey, byte[]> pair : pairs.entrySet()) {
            SwapKey key = pair.getKey();

            if (key != null) {
                space.store(key, pair.getValue());

                notifyListener(EVT_SWAP_SPACE_DATA_STORED, spaceName);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void setListener(@Nullable SwapSpaceSpiListener evictLsnr) {
        this.evictLsnr = evictLsnr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Collection<Integer> partitions(@Nullable String spaceName)
        throws IgniteSpiException {
        Space space = space(spaceName, false);

        if (space == null)
            return null;

        return space.partitions();
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K> IgniteSpiCloseableIterator<K> keyIterator(@Nullable String spaceName,
        SwapContext ctx) throws IgniteSpiException {
        final Space space = space(spaceName, false);

        if (space == null)
            return null;

        final Iterator<Map.Entry<SwapKey, byte[]>> iter = space.entriesIterator();

        return new GridCloseableIteratorAdapter<K>() {
            @Override protected boolean onHasNext() {
                return iter.hasNext();
            }

            @Override protected K onNext() {
                return (K)iter.next().getKey().key();
            }

            @Override protected void onRemove() {
                iter.remove();
            }
        };
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteSpiCloseableIterator<Map.Entry<byte[], byte[]>> rawIterator(
        @Nullable String spaceName) throws IgniteSpiException {
        Space space = space(spaceName, false);

        if (space == null)
            return null;

        return rawIterator(space.entriesIterator());
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteSpiCloseableIterator<Map.Entry<byte[], byte[]>> rawIterator(
        @Nullable String spaceName, int part) throws IgniteSpiException {
        Space space = space(spaceName, false);

        if (space == null)
            return null;

        return rawIterator(space.entriesIterator(part));
    }

    /**
     * Creates raw iterator based on provided entries iterator.
     *
     * @param iter Entries iterator.
     * @return Raw iterator.
     */
    private IgniteSpiCloseableIterator<Map.Entry<byte[], byte[]>> rawIterator(
        final Iterator<Map.Entry<SwapKey, byte[]>> iter) {
        return new GridCloseableIteratorAdapter<Map.Entry<byte[], byte[]>>() {
            @Override protected Map.Entry<byte[], byte[]> onNext() {
                Map.Entry<SwapKey, byte[]> x = iter.next();

                return new T2<>(keyBytes(x.getKey()), x.getValue());
            }

            @Override protected boolean onHasNext() {
                return iter.hasNext();
            }

            @Override protected void onRemove() {
                iter.remove();
            }
        };
    }

    /**
     * Gets key bytes.
     *
     * @param key Swap key.
     * @return Key bytes.
     * @throws org.apache.ignite.spi.IgniteSpiException In case of error.
     */
    private byte[] keyBytes(SwapKey key) throws IgniteSpiException {
        assert key != null;

        byte[] keyBytes = key.keyBytes();

        if (keyBytes == null) {
            try {
                keyBytes = ignite.configuration().getMarshaller().marshal(key.key());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteSpiException("Failed to marshal key: " + key.key(), e);
            }

            key.keyBytes(keyBytes);
        }

        return keyBytes;
    }

    /**
     * Notifies eviction listener.
     *
     * @param evtType Event type.
     * @param spaceName Space name.
     */
    private void notifyListener(int evtType, @Nullable String spaceName) {
        SwapSpaceSpiListener lsnr = evictLsnr;

        if (lsnr != null)
            lsnr.onSwapEvent(evtType, spaceName, null, null);
    }

    /**
     * Gets space by name.
     *
     * @param name Space name.
     * @param create Whether to create space if it doesn't exist.
     * @return Space.
     * @throws org.apache.ignite.spi.IgniteSpiException In case of error.
     */
    @Nullable private Space space(@Nullable String name, boolean create) throws IgniteSpiException {
        String masked = name != null ? name : DFLT_SPACE_NAME;

        assert masked != null;

        Space space = spaces.get(masked);

        if (space == null && create) {
            validateName(name);

            Space old = spaces.putIfAbsent(masked, space = new Space(masked));

            if (old != null)
                space = old;
        }

        if (space != null)
            space.initialize();

        return space;
    }

    /**
     * Validates space name.
     *
     * @param name Space name.
     * @throws org.apache.ignite.spi.IgniteSpiException If name is invalid.
     */
    private void validateName(@Nullable String name) throws IgniteSpiException {
        if (name == null)
            return;

        if (name.isEmpty())
            throw new IgniteSpiException("Space name cannot be empty: " + name);
        else if (DFLT_SPACE_NAME.equalsIgnoreCase(name))
            throw new IgniteSpiException("Space name is reserved for default space: " + name);
        else if (name.contains("/") || name.contains("\\"))
            throw new IgniteSpiException("Space name contains invalid characters: " + name);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FileSwapSpaceSpi.class, this);
    }

    /**
     * Swap value.
     */
    static class SwapValue {
        /** */
        private static final int NEW = 0;

        /** */
        private static final int DELETED = Integer.MIN_VALUE;

        /** */
        private static final AtomicIntegerFieldUpdater<SwapValue> idxUpdater = AtomicIntegerFieldUpdater.
            newUpdater(SwapValue.class, "idx");

        /** */
        private byte[] val;

        /** */
        private final int len;

        /** */
        @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
        private long pos = -1;

        /** */
        @SuppressWarnings("UnusedDeclaration")
        private volatile int idx;

        /**
         * @param val Value.
         */
        SwapValue(byte[] val) {
            assert val != null;

            this.val = val;
            len = val.length;
        }

        /**
         * @param space Space.
         * @return Value.
         * @throws org.apache.ignite.spi.IgniteSpiException If failed.
         */
        @SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
        @Nullable public synchronized byte[] value(Space space) throws IgniteSpiException {
            byte[] v = val;

            if (v == null) { // Read value from file.
                int i = idx;

                assert i != NEW;

                if (i != DELETED) {
                    StripedFileChannel ch = i < 0 ? space.left.readCh : space.right.readCh;

                    if (idx != DELETED) // Double check works in pair with striped channel reopening.
                        v = readValue(ch);
                }
            }
            else if (v.length != len) {
                int p = (int)pos;

                v = Arrays.copyOfRange(v, p, p + len); // In case of compaction.
            }

            return v;
        }

        /**
         * @param ch File channel.
         * @return Bytes.
         * @throws org.apache.ignite.spi.IgniteSpiException if failed.
         */
        @Nullable byte[] readValue(StripedFileChannel ch) throws IgniteSpiException {
            byte[] v = new byte[len];

            int res = 0;

            try {
                res = ch.read(ByteBuffer.wrap(v), pos);
            }
            catch (ClosedByInterruptException e) {
                throw new IgniteSpiException("Operation was interrupted.", e);
            }
            catch (AsynchronousCloseException ignore) {
                assert idx == DELETED; // We closed it ourselves.
            }
            catch (ClosedChannelException e) {
                throw new IgniteSpiException("File channel was unexpectedly closed.", e);
            }
            catch (IOException e) {
                throw new IgniteSpiException("Failed to read value.", e);
            }

            if (res < len)
                return null; // When concurrent compaction occurs this may happen.

            return v;
        }

        /**
         * @param pos Position.
         * @param val Value.
         */
        public synchronized void set(long pos, byte[] val) {
            if (pos != -1)
                this.pos = pos;

            this.val = val;
        }

        /**
         * @param exp Expected.
         * @param idx New index.
         * @return {@code true} if succeeded.
         */
        public boolean casIdx(int exp, int idx) {
            return idxUpdater.compareAndSet(this, exp, idx);
        }

        /**
         * @return Index in file array.
         */
        int idx() {
            return idx;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return pos + " " + len;
        }
    }

    /**
     * Queue of swap values.
     */
    private static class SwapValuesQueue {
        /** */
        private final ArrayDeque<SwapValue> deq = new ArrayDeque<>();

        /** */
        @SuppressWarnings("TypeMayBeWeakened")
        private final ReentrantLock lock = new ReentrantLock();

        /** */
        private final Condition mayAdd = lock.newCondition();

        /** */
        private final Condition mayTake = lock.newCondition();

        /** */
        private int size;

        /** */
        private final int minTakeSize;

        /** */
        private final int maxSize;

        /**
         * @param minTakeSize Min size.
         * @param maxSize Max size.
         */
        private SwapValuesQueue(int minTakeSize, int maxSize) {
            this.minTakeSize = minTakeSize;
            this.maxSize = maxSize;
        }

        /**
         * Adds to queue.
         *
         * @param val Swap value.
         * @throws org.apache.ignite.spi.IgniteSpiException If failed.
         */
        public void add(SwapValue val) throws IgniteSpiException {
            lock.lock();

            try {
                while (size + val.len > maxSize)
                    mayAdd.await();

                size += val.len;

                deq.addLast(val);

                if (size >= minTakeSize)
                    mayTake.signalAll();
            }
            catch (InterruptedException e) {
                throw new IgniteSpiException(e);
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * Takes swap values from queue.
         *
         * @return Swap values.
         * @throws InterruptedException If interrupted.
         */
        public SwapValues take() throws InterruptedException {
            lock.lock();

            try {
                while (size < minTakeSize)
                    mayTake.await();

                int size = 0;
                int cnt = 0;

                for (SwapValue val : deq) {
                    size += val.len;
                    cnt++;

                    if (size >= minTakeSize)
                        break;
                }

                SwapValue[] vals = new SwapValue[cnt];

                for (int i = 0; i < cnt; i++) {
                    SwapValue val = deq.pollFirst();

                    vals[i] = val;
                }

                if ((this.size -= size) < maxSize)
                    mayAdd.signalAll();

                return new SwapValues(vals, size);
            }
            finally {
                lock.unlock();
            }
        }
    }

    /**
     * Array of swap values and their size in bytes.
     */
    static class SwapValues {
        /** */
        private final SwapValue[] vals;

        /** Size in bytes. */
        private final int size;

        /**
         * @param vals Values.
         * @param size Size.
         */
        SwapValues(SwapValue[] vals, int size) {
            this.vals = vals;
            this.size = size;
        }
    }

    /**
     * Readable striped file channel.
     */
    private static class StripedFileChannel {
        /** */
        private final AtomicInteger enter = new AtomicInteger();

        /** */
        private final RandomAccessFile[] rafs;

        /** */
        private final FileChannel[] chs;

        /**
         * @param f File.
         * @param stripes Stripes.
         * @throws FileNotFoundException If failed.
         */
        StripedFileChannel(File f, int stripes) throws FileNotFoundException {
            assert stripes > 0 && (stripes & (stripes - 1)) == 0 : "stripes must be positive and power of two.";

            rafs = new RandomAccessFile[stripes];
            chs = new FileChannel[stripes];

            for (int i = 0; i < stripes; i++) {
                RandomAccessFile raf = new RandomAccessFile(f, "r");

                rafs[i] = raf;
                chs[i] = raf.getChannel();
            }
        }

        /**
         * Reads data from file channel to buffer.
         *
         * @param buf Buffer.
         * @param pos Position.
         * @return Read bytes count.
         * @throws IOException If failed.
         */
        int read(ByteBuffer buf, long pos) throws IOException {
            int i = enter.getAndIncrement() & (chs.length - 1);

            return chs[i].read(buf, pos);
        }

        /**
         * Closes channel.
         */
        void close() {
            for (RandomAccessFile raf : rafs)
                U.closeQuiet(raf);
        }
    }

    /**
     * Swap file.
     */
    static class SwapFile {
        /** */
        private static final long MIN_TRUNK_SIZE = 10 * 1024 * 1024;

        /** */
        private final File file;

        /** */
        private final RandomAccessFile raf;

        /** */
        private final FileChannel writeCh;

        /** */
        volatile StripedFileChannel readCh;

        /** */
        private volatile long len;

        /** */
        private final FileSwapArray<SwapValue> arr = new FileSwapArray<>();

        /**
         * @param file File.
         * @param readerStripes Reader stripes number.
         * @throws IOException In case of error.
         */
        SwapFile(File file, int readerStripes) throws IOException {
            assert file != null;

            file.delete();

            if (!file.createNewFile())
                throw new IllegalStateException("Failed to create file: " + file.getAbsolutePath());

            this.file = file;

            raf = new RandomAccessFile(file, "rw");

            writeCh = raf.getChannel();

            readCh = new StripedFileChannel(file, readerStripes);
        }

        /**
         * Reopens read channel.
         *
         * @throws FileNotFoundException If failed.
         */
        void reopenReadChannel() throws FileNotFoundException {
            readCh.close();

            readCh = new StripedFileChannel(file, readCh.chs.length);
        }

        /**
         * @param vals Values.
         * @param buf Duffer.
         * @param sign Indicates where should we write value, to the left or to the right.
         * @throws Exception If failed.
         */
        public void write(Iterable<SwapValue> vals, ByteBuffer buf, int sign) throws Exception {
            for (SwapValue val : vals) {
                int oldIdx = val.idx;

                if (oldIdx == SwapValue.DELETED)
                    continue;

                int idx = arr.add(val);

                if (!val.casIdx(oldIdx, sign * idx)) {
                    assert val.idx == SwapValue.DELETED;

                    boolean res = tryRemove(idx, val);

                    assert res;
                }
            }

            final int size = buf.remaining();

            if (size == 0)
                return;

            long pos = len;

            len = pos + size;

            long res = writeCh.write(buf, pos);

            if (res != size)
                throw new IllegalStateException(res + " != " + size);

            // Nullify bytes in values ans set pos.
            for (SwapValue val : vals) {
                val.set(pos, null);

                pos += val.len;
            }
        }

        /**
         * @param vals Values.
         * @param sign Sign: 1 or -1.
         * @throws Exception If failed.
         */
        public void write(SwapValues vals, int sign) throws Exception {
            ByteBuffer buf = ByteBuffer.allocateDirect(vals.size);

            for (int i = 0, len = vals.vals.length; i < len; i++) {
                SwapValue val = vals.vals[i];

                if (val.idx == SwapValue.DELETED) {
                    vals.vals[i] = null;

                    continue;
                }

                int idx = arr.add(val);

                if (!val.casIdx(SwapValue.NEW, sign * idx)) {
                    assert val.idx == SwapValue.DELETED;

                    tryRemove(idx, val);

                    vals.vals[i] = null;
                }
                else
                    buf.put(val.value(null));
            }

            buf.flip();

            final int size = buf.remaining();

            if (size == 0)
                return;

            long pos = len;

            len = pos + size;

            long res = writeCh.write(buf, pos);

            if (res != size)
                throw new IllegalStateException(res + " != " + size);

            // Nullify bytes in values ans set pos.
            for (SwapValue val : vals.vals) {
                if (val == null)
                    continue;

                val.set(pos, null);

                pos += val.len;
            }
        }

        /**
         * Gets file path.
         *
         * @return File path.
         */
        public String path() {
            return file.getAbsolutePath();
        }

        /**
         * Gets file length.
         *
         * @return File length.
         */
        public long length() {
            return len;
        }

        /**
         * Deletes file.
         *
         * @return Whether file was actually deleted.
         */
        public boolean delete() {
            U.closeQuiet(raf);

            readCh.close();

            return U.delete(file);
        }

        /**
         * @param idx Index.
         * @param exp Expected value.
         * @return {@code true} If succeeded.
         */
        public boolean tryRemove(int idx, SwapValue exp) {
            assert idx > 0 : idx;

            FileSwapArray.Slot<SwapValue> s = arr.slot(idx);

            return s != null && s.cas(exp, null);
        }

        /**
         * Does compaction for one buffer.
         *
         * @param vals Values.
         * @param bufSize Buffer size.
         * @return Buffer.
         * @throws IOException If failed.
         * @throws InterruptedException If interrupted.
         */
        public ByteBuffer compact(ArrayDeque<SwapValue> vals, final int bufSize) throws IOException,
            InterruptedException {
            assert vals.isEmpty();

            Compact c = new Compact(vals, bufSize);

            c.doCompact();

            return c.result();
        }

        /**
         * Single compaction operation.
         */
        private class Compact {
            /** */
            private final ArrayDeque<SwapValue> vals;

            /** */
            private final int bufSize;

            /** */
            private byte[] bytes;

            /** */
            private ByteBuffer buf;

            /** */
            private long beg = -1;

            /** */
            private long end = -1;

            /** */
            private int compacted;

            /**
             * @param vals Values.
             * @param bufSize Buffer size.
             */
            private Compact(ArrayDeque<SwapValue> vals, final int bufSize) {
                assert vals.isEmpty();

                this.vals = vals;
                this.bufSize = bufSize;
            }

            /**
             * Reads buffer and compacts it.
             *
             * @throws IOException if failed.
             */
            private void readAndCompact() throws IOException {
                assert beg != -1;

                if (buf == null) {
                    bytes = new byte[bufSize];

                    buf = ByteBuffer.wrap(bytes);
                }

                final int pos = buf.position();

                final int lim = (int)(end - beg + pos);

                assert pos >= 0;
                assert pos < lim : pos + " " + lim;
                assert lim <= buf.capacity();

                buf.limit(lim);

                int res = writeCh.read(buf, beg);

                assert res == lim - pos;

                int prevEnd = pos;
                long delta = beg - pos; // To translate from file based positions to buffer based.

                for (int j = vals.size(); j > compacted; j--) {
                    SwapValue val = vals.pollFirst();

                    int valPos = (int)(val.pos - delta);

                    if (prevEnd != valPos) {
                        assert prevEnd < valPos : prevEnd + " " + valPos;

                        U.arrayCopy(bytes, valPos, bytes, prevEnd, val.len);
                    }

                    prevEnd += val.len;

                    vals.addLast(val); // To have values in the same order as in byte buffer.
                }

                assert prevEnd > 0 : prevEnd;

                buf.position(prevEnd);

                end = -1;

                compacted = vals.size();
            }

            /**
             * Compacts.
             *
             * @throws IOException If failed.
             */
            private void doCompact() throws IOException {
                int idx = arr.size();

                while (--idx > 0) {
                    FileSwapArray.Slot<SwapValue> s = arr.slot(idx);

                    assert s != null;

                    SwapValue v = s.get();

                    if (v == null || v.idx == SwapValue.DELETED)
                        continue;

                    if (end == -1)
                        end = v.pos + v.len;

                    long size = end - v.pos;

                    if ((buf == null ? bufSize : buf.remaining()) < size) {
                        if (vals.isEmpty()) {  // Too big single value.
                            assert bytes == null && buf == null;

                            bytes = new byte[(int)size];

                            buf = ByteBuffer.wrap(bytes);
                        }
                        else if (compacted == vals.size())
                            break; // Finish current compaction, nothing new collected.
                        else { // Read region and compact values in buffer.
                            readAndCompact();

                            // Retry the same value.
                            idx++;

                            continue;
                        }
                    }

                    beg = v.pos;

                    vals.addFirst(v);

                    s.cas(v, null);
                }

                if (vals.isEmpty()) {
                    arr.truncate(1);

                    writeCh.truncate(0);

                    len = 0;

                    reopenReadChannel(); // Make sure that value can be read only from right file but not after switch.

                    return;
                }

                if (compacted != vals.size())
                    readAndCompact();

                int pos = 0;

                for (SwapValue val : vals) { // The values will share one byte array with different offsets while moving.
                    val.set(pos, bytes);

                    pos += val.len;
                }

                buf.flip();

                assert buf.limit() == pos : buf.limit() + " " + pos;

                arr.truncate(idx + 1);

                if (len - beg > MIN_TRUNK_SIZE) {
                    writeCh.truncate(beg);

                    len = beg;
                }
            }

            /**
             * @return Buffer.
             */
            public ByteBuffer result() {
                return buf;
            }
        }
    }

    /**
     * Space.
     */
    private class Space {
        /** Space name. */
        private final String name;

        /** */
        private final GridAtomicInitializer<Void> initializer = new GridAtomicInitializer<>();

        /** Swap file left. */
        @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
        private SwapFile left;

        /** Swap file right. */
        @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
        private SwapFile right;

        /** */
        private final SwapValuesQueue que = new SwapValuesQueue(writeBufSize, maxWriteQueSize);

        /** Partitions. */
        private final ConcurrentMap<Integer, ConcurrentMap<SwapKey, SwapValue>> parts =
            new ConcurrentHashMap8<>();

        /** Total size. */
        private final AtomicLong size = new AtomicLong();

        /** Total count. */
        private final AtomicLong cnt = new AtomicLong();

        /** */
        private int sign = 1;

        /** Writer thread. */
        private Thread writer;

        /** */
        private Thread compactor;

        /**
         * @param name Space name.
         */
        private Space(String name) {
            assert name != null;

            this.name = name;
        }

        /**
         * Initializes space.
         *
         * @throws org.apache.ignite.spi.IgniteSpiException If initialization failed.
         */
        public void initialize() throws IgniteSpiException {
            if (initializer.succeeded())
                return;

            assert dir.exists();
            assert dir.isDirectory();

            try {
                initializer.init(new Callable<Void>(){
                    @Override public Void call() throws Exception {
                        left = new SwapFile(new File(dir, name + ".left"), readStripesNum);

                        right = new SwapFile(new File(dir, name + ".right"), readStripesNum);

                        final Object mux = new Object();

                        writer = new IgniteSpiThread(gridName,  "Swap writer: " + name, log) {
                            @Override protected void body() throws InterruptedException {
                                while (!isInterrupted()) {
                                    SwapValues vals = que.take();

                                    synchronized (mux) {
                                        SwapFile f = sign == 1 ? right : left;

                                        try {
                                            f.write(vals, sign);
                                        }
                                        catch (Exception e) {
                                            throw new IgniteException(e);
                                        }
                                    }
                                }
                            }
                        };

                        compactor = new IgniteSpiThread(gridName, "Swap compactor: " + name, log) {
                            @Override protected void body() throws InterruptedException {
                                SwapFile w = null;
                                SwapFile c = null;

                                ArrayDeque<SwapValue> vals = null;

                                while (!isInterrupted()) {
                                    while(!needCompact()) {
                                        LockSupport.park();

                                        if (isInterrupted())
                                            return;
                                    }

                                    ByteBuffer buf = null;

                                    if (vals == null)
                                        vals = new ArrayDeque<>();
                                    else {
                                        vals.clear();

                                        try {
                                            buf = c.compact(vals, writeBufSize);
                                        }
                                        catch (IOException e) {
                                            throw new IgniteException(e);
                                        }
                                    }

                                    if (vals.isEmpty()) {
                                        synchronized (mux) {
                                            sign = -sign;

                                            if (sign == 1) {
                                                w = right;
                                                c = left;
                                            }
                                            else {
                                                w = left;
                                                c = right;
                                            }
                                        }
                                    }
                                    else {
                                        assert buf != null && buf.remaining() != 0;

                                        synchronized (mux) {
                                            try {
                                                w.write(vals, buf, sign);
                                            }
                                            catch (Exception e) {
                                                throw new IgniteException(e);
                                            }
                                        }
                                    }
                                }
                            }
                        };

                        writer.start();
                        compactor.start();

                        return null;
                    }
                });
            }
            catch (IgniteCheckedException e) {
                throw new IgniteSpiException(e);
            }
        }

        /**
         * Gets total space size in bytes.
         *
         * @return Total size.
         */
        public long size() {
            return left.length() + right.length();
        }

        /**
         * Gets total space count.
         *
         * @return Total count.
         */
        public long count() {
            return cnt.get();
        }

        /**
         * @param parts Partitions.
         * @return Total count of keys for given partitions.
         */
        public long count(Set<Integer> parts) {
            long cnt = 0;

            for (Integer part : parts) {
                ConcurrentMap<SwapKey, SwapValue> map = partition(part, false);

                if (map != null)
                    cnt += map.size();
            }

            return cnt;
        }

        /**
         * Clears space.
         *
         * @throws org.apache.ignite.spi.IgniteSpiException If failed.
         */
        public void clear() throws IgniteSpiException {
            Iterator<Map.Entry<SwapKey, byte[]>> iter = entriesIterator();

            while (iter.hasNext())
                remove(iter.next().getKey(), false);
        }

        /**
         * Stops space.
         *
         * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException If interrupted.
         */
        public void stop() throws IgniteInterruptedCheckedException {
            U.interrupt(writer);
            U.interrupt(compactor);

            U.join(writer);
            U.join(compactor);

            left.delete();
            right.delete();
        }

        /**
         * Stores value in space.
         *
         * @param key Key.
         * @param val Value.
         * @throws org.apache.ignite.spi.IgniteSpiException In case of error.
         */
        public void store(final SwapKey key, @Nullable final byte[] val) throws IgniteSpiException {
            assert key != null;

            final ConcurrentMap<SwapKey, SwapValue> part = partition(key.partition(), true);

            assert part != null;

            if (val == null) {
                SwapValue swapVal = part.remove(key);

                if (swapVal != null) {
                    removeFromFile(swapVal);

                    size.addAndGet(-swapVal.len);
                    cnt.decrementAndGet();
                }

                return;
            }

            final SwapValue swapVal = new SwapValue(val);

            SwapValue old = part.put(key, swapVal);

            if (old != null) {
                size.addAndGet(val.length - old.len);

                removeFromFile(old);
            }
            else {
                size.addAndGet(val.length);
                cnt.incrementAndGet();
            }

            que.add(swapVal);
        }

        /**
         * Reads value from space.
         *
         * @param key Key.
         * @return Value.
         * @throws org.apache.ignite.spi.IgniteSpiException In case of error.
         */
        @Nullable public byte[] read(SwapKey key) throws IgniteSpiException {
            assert key != null;

            final Map<SwapKey, SwapValue> part = partition(key.partition(), false);

            if (part == null)
                return null;

            SwapValue swapVal = part.get(key);

            if (swapVal == null)
                return null;

            return swapVal.value(this);
        }

        /**
         * Removes value from space.
         *
         * @param key Key.
         * @param read If value has to be read.
         * @return Value.
         * @throws org.apache.ignite.spi.IgniteSpiException In case of error.
         */
        @Nullable public byte[] remove(SwapKey key, boolean read) throws IgniteSpiException {
            assert key != null;

            final Map<SwapKey, SwapValue> part = partition(key.partition(), false);

            if (part == null)
                return null;

            SwapValue val = part.remove(key);

            if (val == null)
                return null;

            size.addAndGet(-val.len);

            cnt.decrementAndGet();

            byte[] bytes = null;

            if (read) {
                bytes = val.value(this);

                assert bytes != null; // Value bytes were read before removal from file, so compaction can't happen.
            }

            removeFromFile(val);

            return bytes;
        }

        /**
         * @param val Value.
         */
        private void removeFromFile(SwapValue val) {
            for (;;) {
                int idx = val.idx;

                assert idx != SwapValue.DELETED;

                if (val.casIdx(idx, SwapValue.DELETED)) {
                    if (idx != SwapValue.NEW) {
                        SwapFile f = idx > 0 ? right : left;

                        f.tryRemove(Math.abs(idx), val);
                    }

                    break;
                }
            }

            if (needCompact())
                LockSupport.unpark(compactor);
        }

        /**
         * @return {@code true} If compaction needed.
         */
        private boolean needCompact() {
            long fileLen = size();

            return fileLen > writeBufSize && (fileLen - size.get()) / (float)fileLen > maxSparsity;
        }

        /**
         * Gets numbers of partitioned stored in this space.
         *
         * @return Partition numbers.
         */
        public Collection<Integer> partitions() {
            return parts.keySet();
        }

        /**
         * Gets partition map by its number.
         *
         * @param part Partition number.
         * @param create Whether to create partition if it doesn't exist.
         * @return Partition map.
         */
        @Nullable private ConcurrentMap<SwapKey, SwapValue> partition(int part, boolean create) {
            ConcurrentMap<SwapKey, SwapValue> map = parts.get(part);

            if (map == null && create) {
                ConcurrentMap<SwapKey, SwapValue> old = parts.putIfAbsent(part,
                    map = new ConcurrentHashMap<>());

                if (old != null)
                    map = old;
            }

            return map;
        }

        /**
         * @param part Partition.
         * @return Iterator over partition.
         */
        public Iterator<Map.Entry<SwapKey, byte[]>> entriesIterator(int part) {
            Map<SwapKey, SwapValue> partMap = partition(part, false);

            if (partMap == null)
                return Collections.<Map.Entry<SwapKey, byte[]>>emptySet().iterator();

            return transform(partMap.entrySet().iterator());
        }

        /**
         * @return Iterator over all entries.
         */
        public Iterator<Map.Entry<SwapKey, byte[]>> entriesIterator() {
            final Iterator<ConcurrentMap<SwapKey, SwapValue>> iter = parts.values().iterator();

            return transform(F.concat(new Iterator<Iterator<Map.Entry<SwapKey, SwapValue>>>() {
                @Override public boolean hasNext() {
                    return iter.hasNext();
                }

                @Override public Iterator<Map.Entry<SwapKey, SwapValue>> next() {
                    return iter.next().entrySet().iterator();
                }

                @Override public void remove() {
                    throw new UnsupportedOperationException();
                }
            }));
        }

        /**
         * Gets iterator for all entries in space.
         *
         * @param iter Iterator with {@link SwapValue} to transform.
         * @return Entries iterator.
         */
        private Iterator<Map.Entry<SwapKey, byte[]>> transform(final Iterator<Map.Entry<SwapKey,
            SwapValue>> iter) {
            return new Iterator<Map.Entry<SwapKey, byte[]>>() {
                /** */
                private Map.Entry<SwapKey, byte[]> next;

                /** */
                private Map.Entry<SwapKey, byte[]> last;

                {
                    advance();
                }

                @Override public boolean hasNext() {
                    return next != null;
                }

                /**
                 * Gets next entry.
                 */
                private void advance() {
                    while (iter.hasNext()) {
                        Map.Entry<SwapKey, SwapValue> entry = iter.next();

                        byte[] bytes;

                        try {
                            bytes = entry.getValue().value(Space.this);
                        }
                        catch (IgniteSpiException e) {
                            throw new IgniteException(e);
                        }

                        if (bytes != null) {
                            next = new T2<>(entry.getKey(), bytes);

                            break;
                        }
                    }
                }

                @Override public Map.Entry<SwapKey, byte[]> next() {
                    final Map.Entry<SwapKey, byte[]> res = next;

                    if (res == null)
                        throw new NoSuchElementException();

                    next = null;

                    advance();

                    last = res;

                    return res;
                }

                @Override public void remove() {
                    if (last == null)
                        throw new IllegalStateException();

                    try {
                        Space.this.remove(last.getKey(), false);
                    }
                    catch (IgniteSpiException e) {
                        throw new IgniteException(e);
                    }
                    finally {
                        last = null;
                    }
                }
            };
        }
    }
}