/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr.hub.sender.store.fs;

import org.gridgain.grid.*;
import org.gridgain.grid.dr.hub.sender.store.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.thread.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.worker.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.nio.file.StandardOpenOption.*;
import static org.gridgain.grid.dr.hub.sender.store.GridDrSenderHubStoreOverflowMode.*;

/**
 * Data center replication sender hub store implementation which persists data in file system.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * The following configuration parameter is mandatory:
 * <ul>
 * <li>Directory path where persistent store data is saved (see {@link #setDirectoryPath(String)})</li>
 * </ul>
 * <h2 class="header">Optional</h2>
 * The following configuration parameters are optional:
 * <ul>
 * <li>Maximum size of each file where data is stored in bytes (see {@link #setMaxFileSize(long)})</li>
 * <li>Maximum amount of files files which can be used to store data (see {@link #setMaxFilesCount(int)})</li>
 * <li>Checkpoint creation frequency (see {@link #setCheckpointFrequency(long)})</li>
 * <li>Overflow mode defining how store will behave in case of overflow
 *      (see {@link #setOverflowMode(GridDrSenderHubStoreOverflowMode)})</li>
 * <li>Checksum enabled flag (see {@link #setChecksumEnabled(boolean)})</li>
 * <li>Read buffer size (see {@link #setReadBufferSize(int)})</li>
 * </ul>
 * <h2 class="header">Java Example</h2>
 * <pre name="code" class="java">
 * GridDrSenderHubConfiguration cfg = new GridDrSenderHubConfiguration();
 *
 * GridDrSenderHubFsStore store = new GridDrSenderHubFsStore();
 *
 * // Set directory path.
 * store.setDirectoryPath("/my/directory/path");
 *
 * // Set file system store for sender hub.
 * cfg.setStore(store);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * GridDrSenderHubFsStore can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.gridgain.grid.GridConfiguration" singleton="true"&gt;
 *         ...
 *         &lt;property name="drSenderHubConfiguration"&gt;
 *              &lt;bean class="org.gridgain.grid.dr.hub.sender.GridDrSenderHubConfiguration"&gt;
 *                  &lt;property name="store"&gt;
 *                      &lt;bean class="org.gridgain.grid.dr.hub.sender.store.fs.GridDrSenderHubFsStore"&gt;
 *                          &lt;property name="directoryPath" value="/my/directory/path"/&gt;
 *                      &lt;/bean&gt;
 *                  &lt;/property&gt;
 *                 ...
 *              &lt;/bean&gt;
 *          &lt;/property&gt;
 *         ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://www.gridgain.com/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 * @see GridDrSenderHubStore
 */
public class GridDrSenderHubFsStore implements GridDrSenderHubStore, LifecycleAware {
    /** Default maximum amount of files which can be used to store data. */
    public static final int DFLT_MAX_FILES_CNT = 10;

    /** Default maximum amount of files which can be used to store data. */
    public static final int DFLT_MAX_FILE_SIZE = 100 * 1024 * 1024;

    /** Default overflow mode. */
    public static final GridDrSenderHubStoreOverflowMode DFLT_OVERFLOW_MODE = REMOVE_OLDEST;

    /** Default read buffer size. */
    public static final int DFLT_READ_BUF_SIZE = 512 * 1024;

    /** Default checkpoint creation frequency. */
    public static final long DFLT_CHECKPOINT_FREQ = 500L;

    /** Default checksum enabled flag. */
    public static final boolean DFLT_CHECKSUM_ENABLED = true;

    /** */
    private final DataCenterStream[] streamById = new DataCenterStream[256];

    /** */
    private long maxFileSize = DFLT_MAX_FILE_SIZE;

    /** */
    private int readBufSize = DFLT_READ_BUF_SIZE;

    /** */
    private int maxFilesNum = DFLT_MAX_FILES_CNT;

    /** */
    private boolean checksum = DFLT_CHECKSUM_ENABLED;

    /** */
    private long checkPntFreq = DFLT_CHECKPOINT_FREQ;

    /** */
    private GridDrSenderHubStoreOverflowMode overflowMode = DFLT_OVERFLOW_MODE;

    /** */
    private String dirPath;

    /** */
    private Path dir;

    /** */
    private final GridConcurrentSkipListSet<LogFile> files = new GridConcurrentSkipListSet<>();

    /** */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    @GridLoggerResource
    private GridLogger log;

    /** Name of the grid. */
    @GridNameResource
    private String gridName;

    /** */
    private volatile LogFile head;

    /** */
    private GridWorker checkPntWorker;

    /** */
    private ConcurrentLinkedQueue<ByteBuffer> pool = new ConcurrentLinkedQueue<>();

    /** */
    private volatile boolean updatedAfterCheckPnt;

    /**
     * Gets maximum size of each file where data is stored in bytes. Together with {@link #getMaxFilesCount()} this
     * parameter defines maximum amount of space which can be given to the store. Once store size is exceeded, the
     * earliest entries will be overwritten with more recent ones.
     * <p>
     * Defaults to {@link #DFLT_MAX_FILE_SIZE}.
     *
     * @return Maximum size of each file where data is stored in bytes.
     */
    public long getMaxFileSize() {
        return maxFileSize;
    }

    /**
     * Sets maximum size of each file where data is stored in bytes. See {@link #getMaxFileSize()} for more
     * information.
     *
     * @param maxFileSize Maximum size of each file where data is stored in bytes
     */
    public void setMaxFileSize(long maxFileSize) {
        this.maxFileSize = maxFileSize;
    }

    /**
     * Gets maximum amount of files files which can be used to store data. Together with {@link #getMaxFileSize()} this
     * parameter defines maximum amount of space which can be given to the store. Once store size is exceeded, the
     * earliest entries will be overwritten with more recent ones.
     * <p>
     * Defaults to {@link #DFLT_MAX_FILES_CNT}.
     *
     * @return Maximum amount of files files that can be used to store data.
     */
    public int getMaxFilesCount() {
        return maxFilesNum;
    }

    /**
     * Sets maximum amount of files files which can be used to store data. See {@link #getMaxFilesCount()} for more
     * information.
     *
     * @param maxFilesNum Maximum amount of files files which can be used to store data
     */
    public void setMaxFilesCount(int maxFilesNum) {
        this.maxFilesNum = maxFilesNum;
    }

    /**
     * Gets checkpoint creation frequency. Checkpoint is a snapshot of current store metadata. In case of restart
     * store will continue from the last saved checkpoint.
     * <p>
     * Defaults to {@link #DFLT_CHECKPOINT_FREQ}.
     *
     * @return Checkpoint creation frequency
     */
    public long getCheckpointFrequency() {
        return checkPntFreq;
    }

    /**
     * Sets checkpoint creation frequency. See {@link #getCheckpointFrequency()} for more information.
     *
     * @param checkPntFreq Checkpoint creation frequency.
     */
    public void setCheckpointFrequency(long checkPntFreq) {
        this.checkPntFreq = checkPntFreq;
    }

    /**
     * Gets directory path where persistent store data is to be saved. This path can be either an absolute path or
     * a path relative to GridGain home directory.
     *
     * @return Directory path.
     */
    public String getDirectoryPath() {
        return dirPath;
    }

    /**
     * Sets directory path where persistent store data is to be saved. See {@link #getDirectoryPath()} for more
     * information.
     *
     * @param dirPath Directory path.
     */
    public void setDirectoryPath(String dirPath) {
        this.dirPath = dirPath;
    }

    /**
     * Gets overflow mode defining how store will behave in case of overflow.
     * <p>
     * Defaults to {@link #DFLT_OVERFLOW_MODE}.
     *
     * @return Overflow mode.
     */
    public GridDrSenderHubStoreOverflowMode getOverflowMode() {
        return overflowMode;
    }

    /**
     * Sets overflow mode defining how store will behave in case of overflow. See {@link #getOverflowMode()} for more
     * information.
     *
     * @param overflowMode Overflow mode.
     */
    public void setOverflowMode(GridDrSenderHubStoreOverflowMode overflowMode) {
        this.overflowMode = overflowMode;
    }

    /**
     * Gets checksum enabled flag. When set to {@code true} checksum for store data will be calculated so that
     * it will be possible to detect data corruption. When set to {@code false} checksum will not be calculated and
     * persisted data corruption or unintended change may be left unnoticed.
     * <p>
     * Defaults to {@link #DFLT_CHECKSUM_ENABLED}.
     *
     * @return Checksum enabled flag.
     */
    public boolean isChecksumEnabled() {
        return checksum;
    }

    /**
     * Gets checksum enabled flag. See {@link #isChecksumEnabled()} for more information.
     *
     * @param checksum Checksum enabled flag.
     */
    public void setChecksumEnabled(boolean checksum) {
        this.checksum = checksum;
    }

    /**
     * Gets read buffer size. The larger the size the more sorted entries could be read from the disk in one system
     * call, but the more time this read will take.
     * <p>
     * Default to {@link #DFLT_READ_BUF_SIZE}.
     *
     * @return Read buffer size.
     */
    public int getReadBufferSize() {
        return readBufSize;
    }

    /**
     * Sets read buffer size. See {@link #getReadBufferSize()} for more information.
     *
     * @param readBufSize Read buffer size.
     */
    public void setReadBufferSize(int readBufSize) {
        this.readBufSize = readBufSize;
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        A.ensure(readBufSize > 64, "readBufSize > 64");
        A.notNull(overflowMode, "overflowMode");
        A.ensure(maxFileSize > 128, "maxFileSize > 128");
        A.ensure(maxFilesNum > 2, "maxFilesNum > 2");
        A.notNull(dirPath, "dirPath");
        A.ensure(checkPntFreq > 50, "checkPntFreq > 50");

        File dirFile = new File(dirPath);

        if (dirFile.isAbsolute())
            dir = dirFile.toPath();
        else if (U.getGridGainHome() != null)
            dir = new File(U.getGridGainHome(), dirPath).toPath();
        else
            throw new GridException("Cannot resolve path: " + dirPath);

        Collection<Path> checkPoints = Collections.emptyList();

        try {
            if (!Files.exists(dir)) {
                try {
                    Files.createDirectories(dir);
                }
                catch (AccessDeniedException e) {
                    // Possible Windows bug.
                    if (!U.isWindows())
                        throw e;

                    // Wait a bit and retry. Fix for GG-6983.
                    try {
                        Thread.sleep(10);
                    }
                    catch (InterruptedException ignored) {
                        // No-op.
                    }

                    Files.createDirectories(dir);
                }
            }
            else {
                initLogFiles();

                checkPoints = initLastCheckPoint();
            }
        }
        catch (IOException e) {
            throw new GridException(e);
        }

        switchToNextFile(files.isEmpty() ? 0 : (files.last().id + 1));

        checkPntWorker = new CheckPointWorker(gridName, log, checkPoints);

        new GridThread(checkPntWorker).start();
    }

    /**
     * @param fileExt Extension.
     * @return Set of files.
     * @throws IOException If failed.
     */
    private TreeSet<FileWithId> files(final String fileExt) throws IOException {
        DirectoryStream<Path> dirStream = Files.newDirectoryStream(dir, new DirectoryStream.Filter<Path>() {
            @Override public boolean accept(Path entry) {
                return entry.toString().endsWith(fileExt);
            }
        });

        TreeSet<FileWithId> res = new TreeSet<>();

        for (Path f : dirStream)
            res.add(new FileWithId(f));

        return res;
    }

    /**
     * @param id File id.
     * @return Existing file or fake one if none.
     */
    private LogFile fileById(long id) {
        assert id >= 0 : id;

        LogFile fake = new LogFile(id);

        LogFile res = files.ceiling(fake);

        if (res == null || res.id != id)
            res = fake;

        return res;
    }

    /**
     * @return Collection of existing checkpoints.
     * @throws IOException If failed.
     */
    @SuppressWarnings("EmptySynchronizedStatement")
    private synchronized Collection<Path> initLastCheckPoint() throws IOException {
        TreeSet<FileWithId> checkPoints = files(CheckPointWorker.FILE_EXTENSION);

        Iterator<FileWithId> iter = checkPoints.descendingIterator();

        while (iter.hasNext()) {
            FileWithId f = iter.next();

            try {
                ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(Files.newInputStream(f.file)));

                checkMagic(in);

                int streamsNum = in.readInt();

                DataCenterStream[] arr = new DataCenterStream[streamsNum];

                for (int i = 0; i < streamsNum; i++) {
                    byte id = in.readByte();
                    long fileId = in.readLong();
                    LogFile logFile = fileById(fileId);
                    long filePos = in.readLong();

                    LogPos p = new LogPos(logFile, filePos);

                    long redoLogSize = in.readLong();

                    checkMagic(in);

                    DataCenterStream stream = new DataCenterStream(id, p);

                    stream.position(p);
                    stream.redoLogSize.set(redoLogSize);

                    arr[i] = stream;
                }

                for (DataCenterStream stream : arr) {
                    if (streamById[stream.id & 0xff] != null)
                        throw new IOException("Duplicate data center id.");

                    streamById[stream.id & 0xff] = stream;
                }

                synchronized (streamById) {
                    // Visibility.
                }

                if (log.isInfoEnabled())
                    log.info("DR Store initialized from: " + f.file);

                break;
            }
            catch (IOException ex) {
                if (log.isInfoEnabled())
                    log.info("Checkpoint file is corrupted:[id=" + f.id + ", msg=" + ex.getMessage() + "]");

                iter.remove();

                Files.delete(f.file);
            }
        }

        if (checkPoints.isEmpty())
            return Collections.emptyList();

        Collection<Path> res = new ArrayList<>(checkPoints.size());

        for (FileWithId f : checkPoints)
            res.add(f.file);

        return res;
    }

    /**
     * @param in Input stream.
     * @throws IOException If failed.
     */
    private static void checkMagic(ObjectInputStream in) throws IOException {
        if (in.readShort() != CheckPointWorker.MAGIC)
            throw new IOException("Wrong magic.");
    }

    /**
     * @throws IOException If failed.
     */
    private void initLogFiles() throws IOException {
        TreeSet<FileWithId> logs = files(LogFile.EXTENSION);

        if (logs.isEmpty())
            return;

        for (FileWithId f : logs)
            files.add(new LogFile(f.id, f.file));
    }

    /**
     * @return Streams.
     */
    @SuppressWarnings("EmptySynchronizedStatement")
    private Collection<DataCenterStream> streams() {
        synchronized (streamById) {
            // Visibility.
        }

        Collection<DataCenterStream> res = new ArrayList<>();

        for (DataCenterStream s : streamById) {
            if (s != null)
                res.add(s);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        U.cancel(checkPntWorker);
        U.join(checkPntWorker, log);

        for (LogFile f : files) {
            f.stopReads();

            try {
                f.stopWrites();
            }
            catch (GridException e) {
                U.error(log, "Failed to stop file operations: " + f, e);
            }
        }
    }

    /**
     * Clears store.
     *
     * @throws GridException If failed.
     */
    public void clear() throws GridException {
        LogFile newFile = switchToNextFile(head.id + 1);

        if (newFile == null)
            newFile = files.last();

        for (LogFile f : files) {
            if (f.id >= newFile.id)
                break;

            delete(f);
        }
    }

    /**
     * @param id Data center ID.
     * @param create Create if absent.
     * @return Data center.
     */
    private DataCenterStream stream(byte id, boolean create) {
        int i = id & 0xff;

        DataCenterStream ds = streamById[i];

        if (ds == null) {
            synchronized (this) {
                ds = streamById[i];

                if (ds == null && create) {
                    ds = new DataCenterStream(id, headPosition());

                    streamById[i] = ds;
                }
            }
        }

        return ds;
    }

    /**
     * @return Head position.
     */
    private LogPos headPosition() {
        LogFile f = head;

        return new LogPos(f, f.size());
    }

    /**
     * @param file File.
     * @throws GridException If failed.
     * @return {@code true} If succeeded.
     */
    private boolean delete(LogFile file) throws GridException {
        if (files.remove(file)) {
            file.delete();

            return true;
        }

        return false;
    }

    /**
     * @param id Next file id.
     * @return Created file or {@code null} if switch to this file id was done concurrently in another thread.
     * @throws GridException If failed.
     */
    private LogFile switchToNextFile(long id) throws GridException {
        LogFile newFile = new LogFile(id);

        if (!files.add(newFile))
            return null;

        if (head != null && newFile.id < head.id) { // To avoid race condition with clear.
            files.remove(newFile);

            return null;
        }

        try {
            newFile.init();
        }
        catch (IOException ex) {
            U.error(log, "Failed to init file: " + newFile, ex);

            files.remove(newFile);

            try {
                newFile.delete();
            }
            catch (GridException ignored) {
                // No-op.
            }

            throw new GridException(ex);
        }

        head = newFile; // Make visible only after initialization.

        return newFile;
    }

    /** {@inheritDoc} */
    @Override public void store(byte[] dataCenters, byte[] data) throws GridException {
        assert !F.isEmpty(dataCenters);

        EntryIn e = new EntryIn(dataCenters, data);

        int entrySize = e.size();

        for (byte dsId : dataCenters)
            stream(dsId, true); // Init streams before write to avoid race on initial position.

        for (;;) {
            LogFile f = head;

            if (f.write(e))
                break;

            if (files.size() == maxFilesNum) { // We need to remove old files.
                LogFile rmv = files.firstx();

                if (overflowMode == STOP) {
                    Comparable<LogPos> p0 = new LogPos(rmv, Long.MAX_VALUE);

                    for (DataCenterStream s : streams()) {
                        LogPos p1 = s.position();

                        if (p0.compareTo(p1) > 0) // We still have streams using this file.
                            throw new GridDrSenderHubStoreOverflowException();
                    }
                }
                else
                    assert overflowMode == REMOVE_OLDEST;

                if (files.size() == maxFilesNum) // Double check condition after we've got the first file.
                    delete(rmv);
            }

            if (switchToNextFile(f.id + 1) == null)
                files.last().awaitInitialized();
        }

        for (byte dsId : dataCenters)
            stream(dsId, false).incrementSize(entrySize);
    }

    /** {@inheritDoc} */
    @Override public GridDrSenderHubStoreCursor cursor(byte dataCenterId) throws GridException {
        return stream(dataCenterId, true).cursor();
    }

    /**
     * @param dataCenterId Data center ID.
     * @return Log size for passed in data center. Returns {@code -1} if
     *      log is overwhelmed for this data center and full state
     *      transfer required.
     * @throws GridException If failed.
     */
    public long redoBytesSize(byte dataCenterId) throws GridException {
        DataCenterStream ds = stream(dataCenterId, false);

        if (ds == null)
            throw new GridException("Failed to find data center with id: " + dataCenterId);

        return ds.size();
    }

    /**
     * @return Total data size disk (including data already sent).
     */
    public long totalBytes() {
        long totalSize = 0;

        for (LogFile f : files)
            totalSize += f.size();

        return totalSize;
    }

    /**
     * @return Byte buffer.
     */
    private ByteBuffer buffer() {
        ByteBuffer buf = pool.poll();

        if (buf != null)
            return buf;

        buf = ByteBuffer.allocateDirect(readBufSize);

        reset(buf);

        return buf;
    }

    /**
     * @param buf Buffer.
     */
    private void release(ByteBuffer buf) {
        reset(buf);

        pool.offer(buf);
    }

    /**
     * Resets buffer to initial state.
     *
     * @param buf Buffer.
     */
    private static void reset(ByteBuffer buf) {
        buf.position(0);
        buf.limit(0);
    }

    /**
     * @param fut Future.
     * @return Future result.
     * @throws GridException If failed.
     */
    private static int get(Future<Integer> fut) throws GridException {
        try {
            return fut.get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new GridInterruptedException(e);
        }
        catch (ExecutionException e) {
            if (e.getCause() instanceof ClosedChannelException)
                throw new NoDataException(e);

            throw new GridException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDrSenderHubFsStore.class, this, super.toString());
    }

    /**
     *
     */
    private class CheckPointWorker extends GridWorker {
        /** */
        static final short MAGIC = (short)0xEEBA;

        /** */
        static final String FILE_EXTENSION = ".chk";

        /** */
        private final ArrayDeque<Path> checkPoints = new ArrayDeque<>();

        /**
         * @param gridName Grid name.
         * @param log Logger.
         * @param existingCheckPoints Existing checkpoints.
         */
        protected CheckPointWorker(@Nullable String gridName, GridLogger log, Collection<Path> existingCheckPoints) {
            super(gridName, "dr-store-checkpoint", log);

            checkPoints.addAll(existingCheckPoints);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, GridInterruptedException {
            while (!isCancelled()) {
                U.sleep(checkPntFreq);

                if (!updatedAfterCheckPnt)
                    continue;

                updatedAfterCheckPnt = false;

                long t = U.currentTimeMillis();

                Path file = dir.resolve(t + FILE_EXTENSION);

                try {
                    head.fsync();

                    ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(
                        Files.newOutputStream(file, CREATE_NEW, DSYNC)));

                    Collection<DataCenterStream> streams = streams();

                    out.writeShort(MAGIC);
                    out.writeInt(streams.size());

                    LogPos min = null;

                    for (DataCenterStream s : streams) {
                        out.writeByte(s.id);

                        LogPos p = s.position();

                        if (min == null || min.compareTo(p) > 0)
                            min = p;

                        out.writeLong(p.file.id);
                        out.writeLong(p.off);

                        out.writeLong(s.size());
                        out.writeShort(MAGIC);
                    }

                    out.flush();
                    out.close();

                    checkPoints.addLast(file);

                    for (LogFile logFile : files.headSet(min.file, false)) {
                        try {
                            delete(logFile);
                        }
                        catch (GridException e) {
                            U.error(log, "Failed to delete file: " + file, e);
                        }
                    }

                    while (checkPoints.size() > 5)
                        Files.deleteIfExists(checkPoints.pollFirst());
                }
                catch (IOException e) {
                    U.error(log, "Failed to create checkpoint: " + file, e);
                }
            }
        }
    }

    /**
     *
     */
    private class DataCenterStream {
        /** */
        private final byte id;

        /** */
        private final AtomicLong redoLogSize = new AtomicLong();

        /** */
        private final AtomicReference<LogPos> pos;

        /**
         * @param id ID.
         * @param p Current position.
         */
        private DataCenterStream(byte id, LogPos p) {
            assert p != null;

            this.id = id;

            pos = new AtomicReference<>(p);
        }

        /**
         * @return Full log size.
         */
        long size() {
            return redoLogSize.longValue();
        }

        /**
         * @param size Size.
         */
        public void incrementSize(int size) {
            redoLogSize.getAndAdd(size);
        }

        /**
         * @param p Position.
         */
        private void position(LogPos p) {
            for (;;) {
                LogPos old = pos.get();

                if (p.compareTo(old) <= 0 || pos.compareAndSet(old, p))
                    return;
            }
        }

        /**
         * @return Position.
         */
        public LogPos position() {
            return pos.get();
        }

        /**
         * @return Cursor.
         */
        public GridDrSenderHubStoreCursor cursor() {
            return new Cursor(this);
        }
    }

    /**
     *
     */
    private class Cursor implements GridDrSenderHubStoreCursor {
        /** */
        private final DataCenterStream stream;

        /** */
        private final Collection<EntryOut> active = new GridConcurrentSkipListSet<>();

        /** */
        private final ArrayDeque<ByteBuffer> bufs = new ArrayDeque<>();

        /** */
        private LogPos pos;

        /**
         * @param stream Stream.
         */
        Cursor(DataCenterStream stream) {
            this.stream = stream;

            pos = stream.position();

            nextBuffer(); // Init first buffer.
        }

        /**
         * @param buf Buffer.
         * @return {@code true} If switched.
         */
        private boolean switchToNextFile(ByteBuffer buf) {
            LogFile next = files.higher(pos.file);

            if (next == null || next.id > head.id) // No next file or it is not initialized yet.
                return false;

            pos = new LogPos(next, 0);

            reset(buf);

            return true;
        }

        /** {@inheritDoc} */
        @Nullable @Override public GridDrSenderHubStoreEntry next() throws GridException {
            while (bufs.size() > 1)
                release(bufs.pollFirst());

            ByteBuffer buf = bufs.peekLast();

            if (buf == null)
                throw new GridException("Cursor is closed.");

            try {
                for (;;) { // Loop searching for the next entry with our data center id.
                    LogPos curPos = pos;

                    if (buf.remaining() < EntryIn.HEADER_SIZE) { // Read from file.
                        if (curPos.file.size() < curPos.off + EntryIn.HEADER_SIZE) {
                            if (curPos.file.isWritable())
                                return null;

                            if (curPos.file.size() < curPos.off + EntryIn.HEADER_SIZE) { // Double check.
                                if (switchToNextFile(buf))
                                    continue;

                                return null;
                            }
                        }

                        curPos.readMore(buf, buf.remaining(), EntryIn.HEADER_SIZE);
                    }

                    if (buf.getShort() != EntryIn.MAGIC)
                        throw new GridDrSenderHubStoreCorruptedException("Magic mismatch.");

                    int streamsNum = buf.get() & 0xFF;
                    int hash = buf.getInt();
                    int dataSize = buf.getInt();

                    int fullHdrSize = EntryIn.HEADER_SIZE + streamsNum;
                    int sizeOnDisk = fullHdrSize + dataSize;

                    if (buf.remaining() < streamsNum) {
                        if (curPos.file.size() < curPos.off + fullHdrSize) {
                            if (curPos.file.isWritable()) {
                                reset(buf); // Reset buffer to reread header.

                                return null;
                            }

                            if (curPos.file.size() < curPos.off + fullHdrSize) {
                                if (switchToNextFile(buf))
                                    continue;

                                return null;
                            }
                        }

                        curPos.readMore(buf, EntryIn.HEADER_SIZE + buf.remaining(), fullHdrSize);
                    }

                    boolean found = false;

                    for (int i = 1; i <= streamsNum; i++) {
                        if (stream.id == buf.get()) {
                            found = true;

                            buf.position(buf.position() - i + streamsNum);

                            break;
                        }
                    }

                    if (!found) {
                        if (buf.remaining() >= dataSize + EntryIn.HEADER_SIZE) // Contains next header.
                            buf.position(buf.position() + dataSize); // Move position to the next entry begin.
                        else
                            reset(buf); // We will need to read next entry.

                        // Switch position to next entry begin.
                        pos = new LogPos(pos.file, pos.off + sizeOnDisk);

                        continue; // Try next entry.
                    }

                    // Read and return current entry.
                    ByteBuffer[] bufs;

                    if (buf.remaining() < dataSize) {
                        if (curPos.file.size() < curPos.off + sizeOnDisk) {
                            if (curPos.file.isWritable()) {
                                reset(buf);

                                return null;
                            }

                            if (curPos.file.size() < curPos.off + sizeOnDisk) {
                                if (switchToNextFile(buf))
                                    continue;

                                return null;
                            }
                        }

                        int bufsNum = (dataSize - buf.remaining()) / readBufSize + 2;

                        bufs = new ByteBuffer[bufsNum];

                        bufs[0] = buf.asReadOnlyBuffer();

                        int off = EntryIn.HEADER_SIZE + streamsNum + buf.remaining(); // Current offset from position begin.

                        // Read remaining data to the next buffers.
                        for (int i = 1, last = bufsNum - 1; i <= last; i++) {
                            ByteBuffer next = nextBuffer();

                            int remaining = i == last ? dataSize - readBufSize * (bufsNum - 2) - buf.remaining() :
                                readBufSize;

                            curPos.readMore(next, off, off + remaining);

                            off += remaining;

                            bufs[i] = next.asReadOnlyBuffer();

                            if (i == last) {
                                next.position(remaining);
                                bufs[i].limit(remaining);
                            }
                        }
                    }
                    else {
                        int end = buf.position() + dataSize;

                        ByteBuffer buf0 = buf.asReadOnlyBuffer();

                        buf0.limit(end);
                        buf.position(end);

                        bufs = new ByteBuffer[]{buf0};
                    }

                    if (checksum && U.hashCode(bufs) != hash)
                        throw new GridDrSenderHubStoreCorruptedException("Checksum mismatch.");

                    // Switch position to next entry begin.
                    pos = new LogPos(pos.file, pos.off + sizeOnDisk);

                    EntryOut e = new EntryOut(this, bufs, curPos, sizeOnDisk);

                    active.add(e);

                    return e;
                }
            }
            catch (GridException e) {
                reset(buf);

                // Read error can happen because of corruption or because file was removed on overflow -> go to head.
                pos = headPosition();
                stream.position(pos);

                if (e instanceof NoDataException) {
                    assert overflowMode == REMOVE_OLDEST;

                    if (log.isDebugEnabled())
                        log.debug("Store was overflown, oldest file was removed.");

                    return next();
                }

                throw e;
            }
        }

        /**
         * @return Next buffer.
         */
        private ByteBuffer nextBuffer() {
            ByteBuffer buf = buffer();

            bufs.add(buf);

            return buf;
        }

        /**
         * @param entry Entry.
         */
        void acknowledge(EntryOut entry) {
            EntryOut e0 = null;

            for (EntryOut e : active) {
                e0 = e;

                if (e.acked) {
                    if (!active.remove(e))
                        return;
                }
                else {
                    stream.position(e.pos);

                    return;
                }
            }

            if (e0 != null) { // We removed all the active entries.
                LogPos p = e0.pos;

                stream.position(new LogPos(p.file, p.off + e0.len)); // Set to the EOF.
            }
        }

        /** {@inheritDoc} */
        @Override public void close() {
            active.clear();

            for (ByteBuffer buf : bufs)
                release(buf);
        }
    }

    /**
     *
     */
    private static class LogPos implements Comparable<LogPos> {
        /** */
        private final LogFile file;

        /** */
        private final long off;

        /**
         * @param file File.
         * @param off Offset.
         */
        private LogPos(@Nullable LogFile file, long off) {
            this.file = file;
            this.off = off;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(LogPos o) {
            if (file == null)
                return o.file == null ? 0 : -1;

            if (o.file == null)
                return 1;

            int res = file.compareTo(o.file);

            if (res != 0)
                return res;

            return Long.compare(off, o.off);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof LogPos && compareTo((LogPos)obj) == 0;
        }

        /**
         * @param buf Buffer.
         * @param off Offset from this log position.
         * @return NUmber of read bytes.
         * @throws GridException If failed.
         */
        int read(ByteBuffer buf, int off) throws GridException {
            if (!file.exists())
                throw new NoDataException();

            return get(file.read(this.off + off, buf));
        }

        /**
         * @param buf Buffer.
         * @param off Offset from this position.
         * @param end End position relative to this position.
         * @throws GridException If failed.
         */
        void readMore(ByteBuffer buf, int off, int end) throws GridException {
            buf.compact();

            while (off < end) {
                int res0 = read(buf, off);

                if (res0 == -1) {
                    if (!file.exists())
                        throw new NoDataException();

                    throw new IllegalStateException();
                }

                off += res0;
            }

            buf.flip();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LogPos.class, this);
        }
    }

    /**
     *
     */
    private static class FileWithId implements Comparable<FileWithId> {
        /** */
        private final long id;

        /** */
        private final Path file;

        /**
         * @param file File path.
         */
        private FileWithId(Path file) {
            this.file = file;

            String name = file.toFile().getName();

            int dot = name.lastIndexOf('.');

            assert dot != -1;

            String idStr = name.substring(0, dot);

            id = Long.parseLong(idStr);
        }

        /** {@inheritDoc} */
        @Override public int compareTo(FileWithId o) {
            return Long.compare(id, o.id);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof FileWithId && compareTo((FileWithId)obj) == 0;
        }
    }

    /**
     *
     */
    private class LogFile implements Comparable<LogFile> {
        /** */
        static final String EXTENSION = ".blg";

        /** */
        private final long id;

        /** */
        private final AtomicLong acquiredSize;

        /** */
        private volatile long size;

        /** */
        @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
        private Path file;

        /** */
        @GridToStringExclude
        @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
        private FileChannel writeCh;

        /** */
        @GridToStringExclude
        @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
        private AsynchronousFileChannel readCh;

        /** */
        private final CountDownLatch init;

        /**
         * @param id ID.
         */
        LogFile(long id) {
            this.id = id;

            acquiredSize = new AtomicLong();
            init = new CountDownLatch(1);
        }

        /**
         * @param id ID.
         * @param file Existing file.
         * @throws IOException If failed.
         */
        LogFile(long id, Path file) throws IOException {
            this.id = id;
            this.file = file;

            init = null;
            acquiredSize = null;

            readCh = AsynchronousFileChannel.open(file, StandardOpenOption.READ);
            size = readCh.size();
        }

        /**
         * @throws IOException If failed.
         */
        public void init() throws IOException {
            try {
                file = dir.resolve(id + EXTENSION);

                writeCh = FileChannel.open(file, WRITE, CREATE_NEW);
                readCh = AsynchronousFileChannel.open(file, READ);
            }
            finally {
                assert init.getCount() == 1;

                init.countDown();
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LogFile.class, this);
        }

        /** {@inheritDoc} */
        @Override public int compareTo(LogFile o) {
            return Long.compare(id, o.id);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof LogFile && compareTo((LogFile)obj) == 0;
        }

        /**
         * @return Size in bytes.
         */
        long size() {
            return size;
        }

        /**
         * @param e Entry.
         * @return Write offset if succeeded and {@code -1} otherwise.
         * @throws GridException If failed.
         */
        boolean write(EntryIn e) throws GridException {
            int entrySize = e.size();

            for(;;) {
                long s = acquiredSize.get();

                if (s >= maxFileSize)
                    return false;

                if (acquiredSize.compareAndSet(s, s + entrySize))
                    break;
            }

            ByteBuffer[] bufs = e.toBytes();

            long curSize;

            synchronized (this) {
                if (!isWritable())
                    return false;

                try {
                    curSize = size;

                    assert writeCh.position() == curSize;

                    long written = 0;

                    do
                        written += writeCh.write(bufs);
                    while (written < entrySize);
                }
                catch (IOException ex) {
                    U.close(writeCh, log);

                    throw new GridException(ex);
                }

                size = curSize + entrySize;
            }

            updatedAfterCheckPnt = true;

            if (curSize + entrySize >= maxFileSize)
                stopWrites();

            return true;
        }

        /**
         * @throws GridException If failed.
         */
        public synchronized void stopWrites() throws GridException {
            if (!isWritable())
                return;

            try {
                writeCh.force(false);
            }
            catch (IOException e) {
                U.warn(log, "Failed to fsync channel: " + id, e);
            }

            U.close(writeCh, log);
        }

        /**
         * @throws GridException If failed.
         */
        public synchronized void delete() throws GridException {
            U.close(readCh, log);
            U.close(writeCh, log);

            try {
                Files.deleteIfExists(file);
            }
            catch (IOException e) {
                throw new GridException(e);
            }
        }

        /**
         * @throws GridInterruptedException If interrupted.
         */
        public void awaitInitialized() throws GridInterruptedException {
            assert init != null;

            U.await(init);
        }

        /**
         *
         */
        public synchronized void stopReads() {
            U.close(readCh, log);
        }

        /**
         * @param off Offset.
         * @param buf Byte buffer.
         * @return Future.
         */
        public Future<Integer> read(long off, ByteBuffer buf) {
            return readCh.read(buf, off);
        }

        /**
         * @return {@code true} If writable.
         */
        public boolean isWritable() {
            return writeCh != null && writeCh.isOpen();
        }

        /**
         * @return {@code true}
         */
        public boolean exists() {
            return readCh != null && readCh.isOpen();
        }

        /**
         * @throws IOException If failed.
         */
        public synchronized void fsync() throws IOException {
            if (isWritable())
                writeCh.force(true);
        }
    }

    /**
     *
     */
    private class EntryIn {
        /** magic(2) + streamsNum(1) + hash(4) + dataSize(4) */
        static final int HEADER_SIZE = 11;

        /** */
        static final short MAGIC = (short)0x0BAE;

        /** */
        private final byte[] hdr;

        /** */
        private final byte[] data;

        /** */
        private final byte[] streams;

        /** */
        private final int size;

        /**
         * @param streams Streams.
         * @param data Data.
         */
        private EntryIn(byte[] streams, byte[] data) {
            this.data = data;
            this.streams = streams;

            hdr = new byte[HEADER_SIZE];

            size = hdr.length + streams.length + data.length;

            ByteBuffer hdrBuf = ByteBuffer.wrap(hdr);

            hdrBuf.putShort(MAGIC);
            hdrBuf.put((byte)streams.length);
            hdrBuf.putInt(checksum ? Arrays.hashCode(data) : 0);
            hdrBuf.putInt(data.length);
        }

        /**
         * @return Bytes.
         */
        ByteBuffer[] toBytes() {
            return new ByteBuffer[] {ByteBuffer.wrap(hdr), ByteBuffer.wrap(streams), ByteBuffer.wrap(data)};
        }

        /**
         * @return Size.
         */
        int size() {
            return size;
        }

        /**
         * @return Number of streams this entry participates in.
         */
        int streamsNumber() {
            return streams.length;
        }
    }

    /**
     *
     */
    private static class EntryOut implements GridDrSenderHubStoreEntry, Comparable<EntryOut> {
        /** */
        private byte[] data;

        /** */
        private final Cursor cursor;

        /** */
        private final LogPos pos;

        /** */
        private final int len;

        /** */
        private volatile boolean acked;

        /**
         * @param cursor Cursor.
         * @param bufs Buffer.
         * @param pos Position.
         * @param len Length.
         */
        EntryOut(Cursor cursor, ByteBuffer[] bufs, LogPos pos, int len) {
            this.cursor = cursor;
            data = U.readByteArray(bufs);
            this.pos = pos;
            this.len = len;
        }

        /** {@inheritDoc} */
        @Override public byte[] data() {
            return data;
        }

        /** {@inheritDoc} */
        @Override public void acknowledge() {
            assert !acked;

            acked = true;

            cursor.acknowledge(this);
        }

        /** {@inheritDoc} */
        @Override public int compareTo(EntryOut o) {
            return pos.compareTo(o.pos);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof EntryOut && compareTo((EntryOut)obj) == 0;
        }
    }

    /**
     *
     */
    private static class NoDataException extends GridException {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         *
         */
        private NoDataException() {
            super("No data.");
        }

        /**
         * @param cause Cause.
         */
        private NoDataException(Throwable cause) {
            super(cause);
        }
    }
}
