/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.store.local;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.thread.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.tostring.*;
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
import java.util.concurrent.locks.*;

import static java.nio.file.StandardOpenOption.*;

/**
 * Key-value file storage manager for {@link GridCacheFileLocalStore}.
 *
 * @author @java.author
 * @version @java.version
 */
class GridCacheFileLocalStoreFileManager {
    /** */
    private static final ClassLoader LOADER = GridCacheFileLocalStore.class.getClassLoader();

    /** */
    private final GridCacheFileLocalStoreStripedMap map;

    /** */
    private final GridCacheFileLocalStore store;

    /** */
    private final GridMarshaller marsh;

    /** */
    private final Path dir;

    /** 3 files => 2 bit index. */
    private final StoreFile[] files = new StoreFile[3];

    /** Protects readers from file replaces by compaction. */
    private final ReadWriteLock filesLock = new ReentrantReadWriteLock();

    /** */
    private static final int fileIdxShift = 46;

    /** */
    private static final long fileSizeMask = (1L << fileIdxShift) - 1;

    /** */
    private StoreFile curFile;

    /** */
    private final GridWorker writer;

    /** */
    private final GridWorker compactor;

    /** */
    private final AtomicLong xids = new AtomicLong(1);

    /** */
    private long fileIdx;

    /** */
    private long maxCompactIdx;

    /** */
    private final ByteBuffer compactBuf;

    /** */
    private final ByteBuffer writeBuf;

    /** */
    private final Semaphore flushMux = new Semaphore(1);

    /**
     * @param store Local store.
     * @param cacheName Cache name.
     * @param dir Store directory path.
     * @throws IOException If failed.
     * @throws GridException If failed.
     */
    @SuppressWarnings("CallToThreadStartDuringObjectConstruction")
    GridCacheFileLocalStoreFileManager(final GridCacheFileLocalStore store, String cacheName, Path dir) throws IOException,
        GridException {
        this.store = store;

        marsh = store.marsh;

        this.dir = dir;

        compactBuf = ByteBuffer.allocateDirect(store.compactBufSize);
        writeBuf = ByteBuffer.allocateDirect(store.writeBufSize);

        map = new GridCacheFileLocalStoreStripedMap(store.mapSegments, store.mapCap / store.mapSegments);

        if (Files.exists(dir) && Files.isDirectory(dir))
            initFromExistingFiles();
        else
            Files.createDirectories(dir);

        if (curFile == null) {
            int idx = slotForNewFile();

            curFile = files[idx] = new StoreFile(idx, fileIdx++);
        }

        if (store.fsyncDelay <= 0 && store.writeMode == GridCacheFileLocalStoreWriteMode.SYNC)
            writer = null;
        else {
            writer = new GridWorker(store.gridName, "local-store-writer-" + cacheName, store.log) {
                /** */
                private long lastFsync = U.microTime();

                @Override protected void body() {
                    while(!isCancelled()) {
                        long sleep = 0;

                        Lock l = filesLock.readLock();

                        if (!l.tryLock())
                            return; // Stopping.

                        try {
                            StoreFile f = curFile;

                            if (store.writeMode == GridCacheFileLocalStoreWriteMode.ASYNC_BUFFERED)
                                sleep = f.tryFlush();

                            assert sleep >= 0 : sleep;

                            if (store.fsyncDelay > 0) {
                                long awaitToFsync = f.tryFsync(lastFsync);

                                if (sleep == 0 || awaitToFsync < sleep)
                                    sleep = awaitToFsync;

                                if (awaitToFsync == store.fsyncDelay)
                                    lastFsync = U.microTime();
                            }

                            assert sleep >= 0 : sleep;
                        }
                        finally {
                            l.unlock();
                        }

                        if (sleep != 0)
                            LockSupport.parkNanos(sleep * 1000);
                    }
                }
            };

            new GridThread(writer).start();
        }

        compactor = new GridWorker(store.gridName, "local-store-compactor-" + cacheName, store.log) {
            @Override protected void body() {
                while (!isCancelled()) {
                    try {
                        int fromIdx = fileWithBiggestWaste();

                        if (fromIdx == -1) {
                            U.sleep(store.sparcityCheckFreq);

                            continue;
                        }

                        StoreFile from = files[fromIdx];

                        if (curFile == from) { // Need to switch writes to the next file.
                            int newIdx = slotForNewFile();

                            StoreFile newFile = new StoreFile(newIdx, fileIdx++);

                            Lock l = filesLock.writeLock();

                            l.lockInterruptibly(); // Wait until all current writes finished.

                            try {
                                files[newIdx] = newFile;

                                curFile = files[newIdx];

                                // We have to flush current file before the first flush of the new one.
                                flushMux.acquire();
                            }
                            finally {
                                l.unlock();
                            }

                            from.stopWriting(); // Will flush the last buffer.
                        }

                        int toIdx = -1;

                        for (int i = 0; i < files.length; i++) { // Search for an older file first.
                            StoreFile f = files[i];

                            if (i != fromIdx && f != null && f.fileIdx < from.fileIdx && f != curFile) {
                                toIdx = i;

                                break;
                            }
                        }

                        StoreFile to;

                        if (toIdx == -1) { // If none take an empty slot.
                            toIdx = slotForNewFile();

                            to = new StoreFile(toIdx, from);

                            assert to.compactIdx > maxCompactIdx;

                            Lock l = filesLock.writeLock();

                            l.lockInterruptibly(); // Making sure that all remove entries have correct maxCompactId.

                            try {
                                maxCompactIdx = to.compactIdx;

                                files[toIdx] = to;
                            }
                            finally {
                                l.unlock();
                            }
                        }
                        else
                            to = files[toIdx];

                        assert to != null;

                        compact(from, to);

                        to.fsync();

                        Lock l = filesLock.writeLock();

                        l.lockInterruptibly(); // Wait for completion of all operations accessing the file we are removing.

                        try {
                            files[fromIdx] = null;
                        }
                        finally {
                            l.unlock();
                        }

                        // Now 'from' is invisible for others.
                        to.liveSize.addAndGet(from.liveSize.get());

                        from.delete();
                    }
                    catch (GridInterruptedException | InterruptedException | ClosedByInterruptException ignored) {
                        return;
                    }
                    catch (GridException | IOException e) {
                        U.error(store.log, "Failed to compact store, exiting compaction thread.", e);

                        return;
                    }
                }
            }
        };

        new GridThread(compactor).start();
    }

    /**
     * @throws IOException If failed.
     * @throws GridException If failed.
     */
    private void initFromExistingFiles() throws IOException, GridException {
        Collection<Path> paths = new ArrayList<>(3);

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path file : stream) {
                paths.add(file);
            }
        }

        if (paths.isEmpty())
            return;

        if (paths.size() > 3)
            throw new IllegalStateException("Failed to initialize database from directory: " + dir);

        ArrayList<StoreFile> list = new ArrayList<>();

        int idx = 0;

        for (Path file : paths) {
            files[idx] = new StoreFile(idx, file);

            list.add(files[idx]);

            idx++;
        }

        if (list.size() > 1)
            Collections.sort(list);

        if (list.size() == 3 && list.get(1).fileIdx == 0) {
            StoreFile f = list.remove(1);

            assert list.get(0).fileIdx == 0 && list.get(0).compactIdx == f.compactIdx - 1;

            files[f.idx] = null;

            f.delete(); // Old compaction, can delete.
        }

        for (StoreFile f : list)
            initFile(f);

        if (list.size() == 3) { // Need to finish new -> old compaction.
            StoreFile compact = list.get(1);

            try {
                compact(compact, list.get(0));
            }
            catch (InterruptedException e) {
                throw new GridInterruptedException(e);
            }

            files[compact.idx] = null;

            compact.delete();
        }

        curFile = list.get(list.size() - 1);

        curFile.initEntriesBuffer();

        fileIdx = curFile.fileIdx + 1;

        for (StoreFile f : files) {
            if (f != null && f.compactIdx > maxCompactIdx)
                maxCompactIdx = f.compactIdx;
        }
    }

    /**
     * @param f File.
     * @throws IOException If failed.
     * @throws GridException If failed.
     */
    private void initFile(StoreFile f) throws IOException, GridException {
        f.writeCh.position(0);

        GridCacheFileLocalChannelScanner scan = new GridCacheFileLocalChannelScanner(compactBuf, f.writeCh);

        HashMap<Long, List<Op>> txs = new HashMap<>();

        long pos = 0;
        long liveSize = 0;

        try {
            for (;;) {
                short magic = scan.getShort();

                if (magic == EntryType.COMMIT.magic) {
                    long xid = scan.getLong();

                    List<Op> txOps = txs.remove(xid);

                    if (txOps != null) {
                        for (Op op : txOps) {
                            if (op.rmv) {
                                boolean res = map.remove(op.keyHash, op.old.position());

                                assert res;

                                file(op.old.position()).decrementLiveSize(op.old.size());
                            }
                            else {
                                liveSize += op.size;

                                if (op.old == null)
                                    map.add(op.keyHash, op.addr);
                                else {
                                    boolean res = map.replace(op.keyHash, op.old.position(), op.addr);

                                    assert res;

                                    file(op.old.position()).decrementLiveSize(op.old.size());
                                }
                            }
                        }

                        liveSize += 10;
                    }
                }
                else {
                    int keySize;
                    int keyHash;
                    long xid;
                    int skipSize;
                    int entrySize;
                    int hdrSize;

                    if (magic == EntryType.PUT.magic) {
                        keySize = scan.getInt();
                        int valSize = scan.getInt();
                        keyHash = scan.getInt();
                        scan.getInt(); // valHash
                        xid = scan.getLong();

                        skipSize = keySize + valSize;
                        entrySize = PutEntry.HEADER_SIZE + skipSize;
                        hdrSize = PutEntry.HEADER_SIZE;
                    }
                    else if (magic == EntryType.REMOVE.magic) {
                        keySize = scan.getInt();
                        keyHash = scan.getInt();
                        xid = scan.getLong();

                        skipSize = 16 + keySize;
                        entrySize = RmvEntry.HEADER_SIZE + keySize;
                        hdrSize = RmvEntry.HEADER_SIZE;
                    }
                    else
                        throw new GridException("Failed to read entry: [file=" + f + ", magic=" +
                            Integer.toHexString(magic) + "]");

                    if (xids.get() < xid)
                        xids.lazySet(xid);

                    if (!scan.skip(skipSize))
                        break;

                    byte[] key = new byte[keySize];
                    ByteBuffer keyBuf = ByteBuffer.wrap(key);

                    do
                        f.read(keyBuf, pos + hdrSize + keyBuf.position());
                    while (keyBuf.hasRemaining());

                    DataEntry e = findEntry(keyHash, key);

                    if (xid != 0) { // We have a TX.
                        Op op = new Op();

                        op.keyHash = keyHash;
                        op.old = e;
                        op.rmv = magic == EntryType.REMOVE.magic;

                        if (!op.rmv) {
                            op.size = entrySize;
                            op.addr = f.withFileId(pos);
                        }

                        if ((op.rmv && op.old != null) || !op.rmv) { // Skip if its a remove op but entry was not found.
                            List<Op> txOps = txs.get(xid);

                            if (txOps == null)
                                txs.put(xid, txOps = new ArrayList<>());

                            txOps.add(op);
                        }
                    }
                    else if (magic == EntryType.PUT.magic) {
                        liveSize += entrySize;

                        if (e == null)
                            map.add(keyHash, f.withFileId(pos));
                        else {
                            boolean res = map.replace(keyHash, e.position(), f.withFileId(pos));

                            assert res;

                            file(e.position()).decrementLiveSize(e.size());
                        }
                    }
                    else if (e != null) {
                        boolean res = map.remove(keyHash, e.position());

                        assert res;

                        file(e.position()).decrementLiveSize(e.size());
                    }
                }

                pos = scan.position();
            }
        }
        catch (EOFException ignored) {
            // No-op.
        }

        f.fileSize = pos;
        f.writeCh.truncate(pos);
        f.writeCh.position(pos);
        f.liveSize.set(liveSize);
    }

    /**
     * @return Number of live entries.
     */
    public int size() {
        return map.size();
    }

    /**
     *
     */
    private static class Op {
        /** */
        private DataEntry old;

        /** */
        private boolean rmv;

        /** */
        private int keyHash;

        /** */
        private long addr;

        /** */
        private int size;
    }

    /**
     * @throws GridException If failed.
     */
    public void stop() throws GridException {
        Lock l = filesLock.writeLock();

        l.lock();

        try {
            U.cancel(writer);
            U.cancel(compactor);

            join(writer);
            join(compactor);

            assert flushMux.availablePermits() != 0;

            for (int i = 0; i < files.length; i++) {
                StoreFile f = files[i];

                if (f != null) {
                    U.close(f, store.log);

                    files[i] = null;
                }
            }

            curFile = null;
        }
        finally {
            l.unlock();
        }

        map.close();
    }

    /**
     * @param w Worker.
     */
    private static void join(GridWorker w) {
        try {
            U.join(w);
        }
        catch (GridInterruptedException ignored) {
            // No-op.
        }
    }

    /**
     * @param from From file.
     * @param to To file.
     * @return Number of live data entries.
     * @throws IOException If failed.
     * @throws GridException If failed.
     * @throws InterruptedException If failed.
     */
    int compact(StoreFile from, StoreFile to)
        throws IOException, GridException, InterruptedException {
        ByteBuffer buf = compactBuf;

        buf.clear();

        final long fromFileSize = from.fileSize;
        final long toFileSize = to.fileSize;

        long readPos = 0;

        ByteBuffer[] arr = new ByteBuffer[16];

        GridLongList oldNewPos = new GridLongList();

        int entryRemaining = 0;

        long skipped = 0;

        int liveCnt = 0;

        GridLongList txs = new GridLongList();

        while (readPos < fromFileSize) {
            final int off = buf.position();

            int read = from.read(buf, readPos);

            assert read > 0 : read;

            buf.flip();

            int idx = -1;

            if (entryRemaining >= buf.remaining()) { // Entry larger than buffer.
                assert off == 0;

                entryRemaining -= buf.remaining();

                while (buf.hasRemaining())
                    to.fileSize += to.writeCh.write(buf);
            }
            else if (entryRemaining != 0) { // Handle tail.
                assert off == 0;

                ByteBuffer remainingBuf = buf.asReadOnlyBuffer();

                remainingBuf.limit(entryRemaining);
                buf.position(entryRemaining);

                arr[++idx] = remainingBuf;

                entryRemaining = 0;
            }

            while (buf.remaining() >= 10) { // Full commit entry at least.
                final int pos = buf.position();

                int entrySize;
                boolean skip;

                short magic = buf.getShort();

                long entryAddr = readPos - off + pos;

                long keyHash = Long.MAX_VALUE;

                if (magic == EntryType.COMMIT.magic) {
                    entrySize = 10;

                    long xid = buf.getLong();

                    skip = txs.removeValue(0, xid) == -1;
                }
                else if (magic == EntryType.PUT.magic) {
                    if (buf.remaining() + 2 < PutEntry.HEADER_SIZE) {
                        buf.position(pos);

                        break; // Try to read more.
                    }

                    int keySize = buf.getInt();
                    int valSize = buf.getInt();
                    keyHash = buf.getInt();
                    buf.getInt(); // valHash
                    long xid = buf.getLong();

                    if (xid != 0 && !txs.contains(xid))
                        txs.add(xid);

                    entrySize = keySize + valSize + PutEntry.HEADER_SIZE;

                    skip = !map.contains((int)keyHash, from.withFileId(entryAddr));
                }
                else if (magic == EntryType.REMOVE.magic) {
                    if (buf.remaining() + 2 < RmvEntry.HEADER_SIZE) {
                        buf.position(pos);

                        break; // Try to read more.
                    }

                    int keySize = buf.getInt();
                    keyHash = buf.getInt();
                    long xid = buf.getLong();
                    long fileIdx = buf.getLong();
                    long compactIdx = buf.getLong();

                    entrySize = keySize + RmvEntry.HEADER_SIZE;

                    if (xid != 0 && !txs.contains(xid))
                        txs.add(xid);

                    skip = true;

                    // Can skip when original put entry compacted (no files potentially containing the put exist).
                    for (StoreFile f : files) {
                        if (f == null)
                            continue;

                        if ((f.fileIdx < fileIdx && f.compactIdx == 0) ||
                            (f.fileIdx == 0 && f.compactIdx <= compactIdx)) {
                            skip = false;

                            break;
                        }
                    }
                }
                else
                    throw new GridException("Failed to read entry: [addr=" + entryAddr + ", magic=" +
                        Integer.toHexString(magic) + "]");

                if (skip)
                    skipped += entrySize;
                else if (keyHash != Long.MAX_VALUE) {
                    liveCnt++;

                    oldNewPos.add(keyHash);
                    oldNewPos.add(from.withFileId(entryAddr));
                    oldNewPos.add(to.withFileId(toFileSize + entryAddr - skipped));
                }

                buf.position(pos);

                ByteBuffer entryBuf = null;

                if (buf.remaining() >= entrySize) {
                    if (!skip) {
                        entryBuf = buf.asReadOnlyBuffer();

                        assert entryBuf.position() == pos;

                        entryBuf.limit(pos + entrySize);
                    }

                    buf.position(pos + entrySize);
                }
                else {
                    if (skip)
                        read += entrySize - buf.remaining();
                    else {
                        entryBuf = buf.asReadOnlyBuffer();

                        assert entryBuf.position() == pos;

                        entryRemaining = entrySize - entryBuf.remaining();
                    }

                    buf.position(buf.limit());
                }

                if (entryBuf != null) {
                    if (idx < 0 || arr[idx].limit() != entryBuf.position()) {
                        idx++;

                        if (arr.length == idx)
                            arr = Arrays.copyOf(arr, arr.length * 2);

                        arr[idx] = entryBuf;
                    }
                    else // Merge buffer with previous.
                        arr[idx].limit(entryBuf.limit());
                }
            }

            if (idx != -1) {
                while (arr[idx].hasRemaining())
                    to.fileSize += to.writeCh.write(arr, 0, idx + 1);
            }

            if (!oldNewPos.isEmpty()) {
                for (int i = 0, len = entryRemaining != 0 ? oldNewPos.size() - 3 : oldNewPos.size(); i < len; i += 3) {
                    int keyHash = (int)oldNewPos.get(i);
                    long oldAddr = oldNewPos.get(i + 1);
                    long newAddr = oldNewPos.get(i + 2);

                    map.replace(keyHash, oldAddr, newAddr); // If replace fails, entry was concurrently updated.
                }

                if (entryRemaining != 0)
                    oldNewPos.truncate(3, false); // Leave the last entry.
                else
                    oldNewPos.truncate(0, true);
            }

            buf.compact();

            readPos += read;

            if (Thread.interrupted())
                throw new InterruptedException();
        }

        assert to.fileSize == to.writeCh.size();

        return liveCnt;
    }

    /**
     * @return Free index in array for new file.
     */
    private int slotForNewFile() {
        for (int i = 0; i < files.length; i++) {
            if (files[i] == null)
                return i;
        }

        throw new IllegalStateException();
    }

    /**
     * @return Index of the file which we need to compact with the biggest waste or {@code -1} if none.
     */
    private int fileWithBiggestWaste() {
        long maxWaste = 0;
        int maxWasteIdx = -1;

        // Find file with biggest waste.
        for (int i = 0; i < files.length; i++) {
            StoreFile f = files[i];

            if (f != null && f.needCompact()) {
                long waste = f.waste();

                if (waste > maxWaste) {
                    maxWaste = waste;
                    maxWasteIdx = i;
                }
            }
        }

        return maxWasteIdx;
    }

    /**
     * @param keys Keys.
     * @param clo Closure.
     * @throws GridException If failed.
     */
    public <K, V> void loadAll(Collection<? extends K> keys, final GridBiInClosure<K, V> clo) throws GridException {
        final int len = keys.size();

        int[] hashes = new int[len];
        byte[][] keysBytes = new byte[len][];
        byte[][] valBytes = new byte[len][];

        int i = 0;

        for (Object k : keys) {
            hashes[i] = k.hashCode();
            keysBytes[i++] = marsh.marshal(k);
        }

        Lock l = filesLock.readLock();

        l.lock();

        try {
            i = 0;

            while (i < hashes.length) {
                DataEntry e = findEntry(hashes[i], keysBytes[i]);

                if (e != null)
                    valBytes[i] = e.value();

                i++;
            }
        }
        finally {
            l.unlock();
        }

        i = 0;

        for (K k : keys) {
            clo.apply(k, valBytes[i] == null ? null : marsh.<V>unmarshal(valBytes[i], LOADER));

            i++;
        }
    }

    /**
     * @param clo Closure to apply to keys and values.
     * @throws GridException If failed.
     */
    public <K, V> void loadAll(final GridBiInClosure<K, V> clo) throws GridException {
        Lock l = filesLock.readLock();

        l.lock();

        try {
            map.iterate(new GridCacheFileLocalStoreMap.Closure() {
                @Override public void apply(long addr) throws GridException {
                    DataEntry e = file(addr).read(addr);

                    if (e == null)
                        throw new GridException("Failed to find entry. Store is corrupted.");

                    K k = marsh.unmarshal(e.key(), LOADER);
                    V v = marsh.unmarshal(e.value(), LOADER);

                    clo.apply(k, v);
                }
            });
        }
        finally {
            l.unlock();
        }
    }

    /**
     * @param key Key.
     * @return Value or {@code null} if none.
     * @throws GridException If failed.
     */
    @Nullable public <K, V> V load(K key) throws GridException {
        int hash = key.hashCode();

        byte[] keyBytes = marsh.marshal(key);

        byte[] val = null;

        Lock l = filesLock.readLock();

        l.lock();

        try {
            DataEntry e = findEntry(hash, keyBytes);

            if (e == null)
                return null;

            val = e.value();
        }
        finally {
            l.unlock();
        }

        return marsh.unmarshal(val, LOADER);
    }

    /**
     * @param keyHash Key hash.
     * @param keyBytes Key.
     * @return Entry or {@code null} if none.
     * @throws GridException If failed.
     */
    @Nullable private DataEntry findEntry(int keyHash, byte[] keyBytes) throws GridException {
        GridLongList res = map.get(keyHash);

        if (res == null)
            return null;

        for (int i = 0, len = res.size(); i < len; i++) {
            long addr = res.get(i);

            DataEntry e = file(addr).read(addr);

            if (e.keyEquals(keyBytes, keyHash))
                return e;
        }

        return null;
    }

    /**
     * @param delta Delta to apply.
     * @param xid Transaction ID.
     * @throws GridException If failed.
     */
    public <K, V> void updateAll(Collection<? extends Map.Entry<? extends K, ? extends V>> delta,
        @Nullable GridUuid xid) throws GridException {
        int deltaSize = delta.size();

        assert deltaSize > 0;

        if (deltaSize == 1) {
            for (Map.Entry<? extends K, ? extends V> e : delta)
                update(e.getKey(), e.getValue());

            return;
        }

        // If we have no xid, we will not write a commit entry.
        long xid0 = xid == null ? 0 : xids.incrementAndGet();

        Lock l = filesLock.readLock();

        l.lock();

        try {
            CompoundEntry entry = new CompoundEntry(xid0 == 0 ? deltaSize : deltaSize + 1);

            StoreFile file = curFile;

            Object[] vals = new Object[deltaSize];
            DataEntry[] olds = new DataEntry[deltaSize];
            int[] hashes = new int[deltaSize];
            byte[][] keys = new byte[deltaSize][];

            int c = 0;

            for (Map.Entry<? extends K, ? extends V> e : delta) {
                K key = e.getKey();
                V val = e.getValue();

                int keyHash = key.hashCode();

                byte[] keyBytes = marsh.marshal(key);

                DataEntry old = findEntry(keyHash, keyBytes);

                if (old == null && val == null)
                    continue; // Trying to remove non existing entry.

                olds[c] = old;
                keys[c] = keyBytes;
                hashes[c] = keyHash;
                vals[c] = val;

                c++;

                if (val == null)
                    entry.add(new RmvEntry(xid0, keyBytes, keyHash, file.fileIdx, maxCompactIdx));
                else
                    entry.add(new PutEntry(keyBytes, keyHash, marsh.marshal(val), xid0, store.checksum));
            }

            if (xid0 != 0)
                entry.add(new CommitEntry(xid0));

            long addr = file.write(entry);

            for (int i = 0; i < c; i++) {
                updateMap(vals[i], olds[i], hashes[i], keys[i], addr);

                addr += entry.entries.get(i).len;
            }
        }
        finally {
            l.unlock();
        }
    }

    /**
     * @param val New value or {@code null} for remove.
     * @param old Old entry being updated or {@code null} for put.
     * @param keyHash Key hash.
     * @param keyBytes Key bytes.
     * @param addr New entry address.
     * @throws GridException If failed.
     */
    private void updateMap(@Nullable Object val, @Nullable DataEntry old, int keyHash, byte[] keyBytes, long addr)
        throws GridException {
        assert val != null || old != null;

        for (;;) { // Loop since map update can fail because of compaction.
            if (val == null) { // Remove.
                if (old == null) // Old can become null in this loop -> it was concurrently removed by someone else.
                    break;

                if (map.remove(keyHash, old.position())) {
                    file(old.position()).decrementLiveSize(old.size());

                    break;
                }
            }
            else if (old == null) { // Put non existing key (may be after failed replace).
                map.add(keyHash, addr);

                break;
            }
            else if (map.replace(keyHash, old.position(), addr)) {
                file(old.position()).decrementLiveSize(old.size());

                break;
            }

            old = findEntry(keyHash, keyBytes);
        }
    }

    /**
     * @param keys Keys.
     * @throws GridException If failed.
     */
    public <K, V> void removeAll(final Collection<? extends K> keys) throws GridException {
        if (keys.size() == 1) {
            update(keys.iterator().next(), null);

            return;
        }

        updateAll(new AbstractCollection<Map.Entry<K, V>>() {
            @NotNull @Override public Iterator<Map.Entry<K, V>> iterator() {
                final Iterator<? extends K> i = keys.iterator();

                return new Iterator<Map.Entry<K, V>>() {
                    @Override public boolean hasNext() {
                        return i.hasNext();
                    }

                    @Override public Map.Entry<K, V> next() {
                        final K k = i.next();

                        return new Map.Entry<K, V>() {
                            @Override public K getKey() {
                                return k;
                            }

                            @Override public V getValue() {
                                return null;
                            }

                            @Override public V setValue(V val) {
                                throw new UnsupportedOperationException();
                            }

                            @Override public boolean equals(Object o) {
                                throw new UnsupportedOperationException();
                            }

                            @Override public int hashCode() {
                                throw new UnsupportedOperationException();
                            }
                        };
                    }

                    @Override public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override public int size() {
                return keys.size();
            }
        }, null);
    }

    /**
     * @param key Key.
     * @param val Value or {@code null} if it was removed.
     * @throws GridException If failed.
     */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    public void update(Object key, @Nullable Object val) throws GridException {
        int keyHash = key.hashCode();

        byte[] keyBytes = marsh.marshal(key);

        Lock l = filesLock.readLock();

        l.lock();

        try {
            DataEntry old = findEntry(keyHash, keyBytes);

            if (old == null && val == null)
                return; // Trying to remove non existing entry.

            StoreFile file = curFile;

            AbstractFileEntry entry;

            if (val == null)
                entry = new RmvEntry(0, keyBytes, keyHash, file.fileIdx, maxCompactIdx);
            else
                entry = new PutEntry(keyBytes, keyHash, marsh.marshal(val), 0, store.checksum);

            long addr = file.write(entry);

            updateMap(val, old, keyHash, keyBytes, addr);
        }
        finally {
            l.unlock();
        }
    }

    /**
     * @param addr Address.
     * @return File.
     */
    StoreFile file(long addr) {
        StoreFile res = files[(int)(addr >>> fileIdxShift)];

        assert res != null : Long.toBinaryString(addr) + " " + Integer.toBinaryString((int)(addr >>> fileIdxShift));

        return res;
    }

    /**
     * @param ch Channel.
     * @param path Path.
     * @throws IOException If failed.
     */
    private void lock(FileChannel ch, Path path) throws IOException {
        try {
            FileLock lock = null;

            try {
                lock = ch.tryLock();
            }
            catch (OverlappingFileLockException ignored) {
                // No-op.
            }

            if (lock == null)
                throw new IOException(
                    "Failed to lock file (probably it is in use by another store instance): " + path);
        }
        catch (IOException e) {
            U.close(ch, store.log);

            throw e;
        }
    }

    /**
     *
     */
    @SuppressWarnings("PackageVisibleInnerClass")
    class StoreFile implements Closeable, Comparable<StoreFile> {
        /** */
        private final FileChannel writeCh;

        /** */
        private volatile long fileSize;

        /** */
        private final AtomicLong liveSize;

        /** */
        private final AsynchronousFileChannel readCh;

        /** Buffer available for writes. */
        private volatile EntriesBuffer curBuf;

        /** Flipped buffer which we are writing but it is still readable. */
        private volatile EntriesBuffer flippedBuf;

        /** Index in file array. */
        private final int idx;

        /** Unique index of file. */
        private final long fileIdx;

        /** Compaction index of file. */
        private final long compactIdx;

        /** File path. */
        private final Path path;

        /**
         * @param idx Index of file in file array.
         * @param fileIdx Unique index of file.
         * @throws IOException If failed.
         */
        StoreFile(int idx, long fileIdx) throws IOException {
            this(idx, fileIdx, 0);

            initEntriesBuffer();
        }

        /**
         * Initialize buffer for entries to allow writes.
         */
        void initEntriesBuffer() {
            assert curBuf == null;

            curBuf = new EntriesBuffer(withFileId(fileSize), store.writeBufSize);
        }

        /**
         * @param idx Index of file in file array.
         * @param fileIdx Unique index of file.
         * @param compactIdx Compact index.
         * @throws IOException If failed.
         */
        StoreFile(int idx, long fileIdx, long compactIdx) throws IOException {
            this.idx = idx;
            this.fileIdx = fileIdx;
            this.compactIdx =  compactIdx;

            path = dir.resolve(String.valueOf(fileIdx + "_" + compactIdx));

            EnumSet<StandardOpenOption> writeOptions = EnumSet.of(CREATE_NEW, APPEND);

            if (store.fsyncDelay == 0)
                writeOptions.add(DSYNC);

            writeCh = FileChannel.open(path, writeOptions);

            lock(writeCh, path);

            readCh = AsynchronousFileChannel.open(path, READ);

            liveSize = new AtomicLong();
        }

        /**
         * @param idx Index of file in file array.
         * @param from Initialize from.
         * @throws IOException If failed.
         */
        StoreFile(int idx, StoreFile from) throws IOException {
            this(idx, from.fileIdx, from.compactIdx + 1);
        }

        /**
         * @param idx Index of file in file array.
         * @param path Existing file path.
         * @throws IOException If failed.
         */
        StoreFile(int idx, Path path) throws IOException {
            this.idx = idx;
            this.path = path;

            String name = path.getFileName().toString();

            String[] res = name.split("_");

            fileIdx = Long.parseLong(res[0]);
            compactIdx = Long.parseLong(res[1]);

            EnumSet<StandardOpenOption> writeOptions = EnumSet.of(WRITE, READ);

            if (store.fsyncDelay == 0)
                writeOptions.add(DSYNC);

            writeCh = FileChannel.open(path, writeOptions);

            lock(writeCh, path);

            readCh = AsynchronousFileChannel.open(path, READ);

            fileSize = writeCh.size();
            writeCh.position(fileSize);

            liveSize = new AtomicLong();
        }

        /** {@inheritDoc} */
        @Override public int compareTo(StoreFile o) {
            int res = Long.compare(fileIdx, o.fileIdx);

            if (res == 0)
                res = Long.compare(compactIdx, o.compactIdx);

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return path.getFileName().toString() + "-" + idx;
        }

        /**
         * Flushes and disables the file for writing.
         *
         * @throws GridInterruptedException If interrupted.
         * @throws IOException If failed.
         */
        void stopWriting() throws GridInterruptedException, IOException {
            assert flushMux.availablePermits() == 0 : "flushMux must be acquired";

            for (;;) {
                EntriesBuffer buf = curBuf;

                if (buf == null) {
                    flushMux.release(); // Release before break only if we failed to flush.

                    break;
                }

                if (flush(buf, null, true))
                    break;
                else
                    buf.awaitFlushed();
            }
        }

        /**
         * Removes file.
         * @throws IOException If failed.
         */
        void delete() throws IOException {
            close();

            Files.delete(path);
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            flushMux.acquireUninterruptibly();

            try {
                stopWriting();
            }
            catch (GridInterruptedException ignored) {
                // No-op.
            }

            U.close(writeCh, store.log);
            U.close(readCh, store.log);
        }

        /**
         * @param buf Buffer.
         * @param addr Address.
         * @return Read count.
         * @throws GridException If failed.
         */
        private int read(ByteBuffer buf, long addr) throws GridException {
            int res = U.get(readCh.read(buf, addr));

            if (res == -1)
                throw new GridException("Failed to read data from: " + addr);

            return res;
        }

        /**
         * @param buf Entries buffer.
         * @param addr Address.
         * @return Entry.
         */
        @Nullable private DataEntry findInBuffer(EntriesBuffer buf, long addr) {
            if (buf != null && addr >= buf.bufPos) {
                assert addr < buf.bufPos + store.writeBufSize : Long.toBinaryString(addr) + " " +
                    Long.toBinaryString(idx) + " " + Long.toBinaryString(buf.bufPos);

                // Search in buffer which was not written yet.
                return (DataEntry)buf.find(addr);
            }

            return null;
        }

        /**
         * @param addr Address of entry.
         * @return Entry or {@code null} if none.
         * @throws GridException If failed.
         */
        @Nullable DataEntry read(final long addr) throws GridException {
            DataEntry res = findInBuffer(curBuf, addr);

            if (res != null)
                return res;

            res = findInBuffer(flippedBuf, addr);

            if (res != null)
                return res;

            final long addr0 = addr & fileSizeMask; // Relative to file.

            if (fileSize < addr0 + PutEntry.HEADER_SIZE)
                throw new GridException("Failed to read entry, missing size: " + (addr0 + PutEntry.HEADER_SIZE -
                    fileSize) + " file " + this);

            final ByteBuffer buf = ByteBuffer.allocate(store.readBufSize);

            while (buf.position() < PutEntry.HEADER_SIZE)
                read(buf, addr0 + buf.position());

            buf.flip();

            short magic = buf.getShort();

            if (magic != EntryType.PUT.magic)
                throw new GridException("Magic is wrong: [magic=" + Integer.toHexString(magic) + ", file=" + this + "]");

            final int keySize = buf.getInt();
            final int valSize = buf.getInt();
            final int keyHash = buf.getInt();
            final int valHash = buf.getInt();

            int entrySize = PutEntry.HEADER_SIZE + keySize + valSize;

            if (fileSize < addr0 + entrySize)
                throw new GridException("Failed to read entry, missing size: " + (addr0 + entrySize - fileSize));

            return new DataEntry() {
                /** */
                private byte[] key;

                /** */
                private byte[] val;

                @Override public int keyHash() {
                    return keyHash;
                }

                @Override public byte[] key() throws GridException {
                    if (key != null)
                        return key;

                    assert valSize != 0;

                    byte[] k = new byte[keySize];

                    buf.position(PutEntry.HEADER_SIZE);

                    int availKeyData = buf.limit() - PutEntry.HEADER_SIZE;

                    if (availKeyData > keySize)
                        availKeyData = keySize;

                    if (availKeyData > 0)
                        buf.get(k, 0, availKeyData);

                    if (availKeyData < keySize) {
                        ByteBuffer keyBuf = ByteBuffer.wrap(k);

                        if (availKeyData > 0)
                            keyBuf.position(availKeyData);

                        long keyOff = addr0 + PutEntry.HEADER_SIZE;

                        while(keyBuf.hasRemaining())
                            read(keyBuf, keyOff + keyBuf.position());
                    }

                    key = k;

                    return k;
                }

                @Nullable @Override public byte[] value() throws GridException {
                    if (valSize == 0)
                        return null;

                    if (val != null)
                        return val;

                    byte[] v = new byte[valSize];

                    int availValData = buf.limit() - keySize - PutEntry.HEADER_SIZE;

                    if (availValData > valSize)
                        availValData = valSize;

                    if (availValData > 0) {
                        buf.position(PutEntry.HEADER_SIZE + keySize);

                        buf.get(v, 0, availValData);
                    }

                    if (availValData < valSize) {
                        ByteBuffer valBuf = ByteBuffer.wrap(v);

                        if (availValData > 0)
                            valBuf.position(availValData);

                        long valOff = addr0 + PutEntry.HEADER_SIZE + keySize;

                        while(valBuf.hasRemaining())
                            read(valBuf, valOff + valBuf.position());
                    }

                    if (store.checksum && valHash != Arrays.hashCode(v))
                        throw new GridException("Value checksum mismatch.");

                    val = v;

                    return v;
                }

                @Override public boolean keyEquals(byte[] key, int kHash) throws GridException {
                    if (key.length != keySize || keyHash != kHash)
                        return false;

                    if (this.key != null)
                        return Arrays.equals(this.key, key);

                    int i = 0;
                    int len = Math.min(buf.limit() - PutEntry.HEADER_SIZE, key.length);

                    byte[] bytes = buf.array();

                    while(i < len) {
                        if (bytes[i + PutEntry.HEADER_SIZE] != key[i])
                            return false;

                        i++;
                    }

                    if (len < keySize) {
                        bytes = key(); // Here we have to load whole key.

                        for (i = len; i < keySize; i++)
                            if (bytes[i] != key[i])
                                return false;
                    }

                    return true;
                }

                @Override public long position() {
                    return addr;
                }

                @Override public int size() {
                    return PutEntry.HEADER_SIZE + keySize + valSize;
                }
            };
        }

        /**
         * @return {@code true} If need compact.
         */
        boolean needCompact() {
            long size = fileSize;

            return size >= store.minCompactSize &&
                (size - liveSize.get()) / (double)size >= store.maxSparsity;
        }

        /**
         * @param buf Buffer to flush.
         * @param extra Extra large entry.
         * @param stop Stop writing after this flush.
         * @return {@code true} If we flipped and flushed the buffer successfully.
         * @throws IOException If failed.
         */
        boolean flush(EntriesBuffer buf, AbstractFileEntry extra, boolean stop) throws IOException {
            if (!buf.flip())
                return false;

            int size = buf.size();
            int liveSize = 0;

            long newPos = buf.bufPos + size;

            if (extra != null) {
                extra.pos = newPos;

                newPos += extra.len;

                size += extra.len;

                if (extra.type() != EntryType.REMOVE)
                    liveSize += extra.len;
            }

            boolean success = false;

            if (!stop)
                flushMux.acquireUninterruptibly();

            assert flushMux.availablePermits() == 0 : stop;

            try {
                assert fileSize == writeCh.size() : fileSize + " " + writeCh.size();

                flippedBuf = buf;

                curBuf = stop ? null : new EntriesBuffer(newPos, store.writeBufSize);

                if (size > 0) {
                    buf.awaitAllArrived();

                    liveSize += buf.liveSize();

                    this.liveSize.addAndGet(liveSize);

                    fileSize += size;

                    writeBuf.clear();

                    buf.serializeTo(writeBuf);

                    assert writeBuf.position() == buf.size() : writeBuf + " " + buf;

                    int off = 0;

                    if (extra != null)
                        off = extra.serializeTo(writeBuf, off);

                    int done = 0;

                    writeBuf.flip();
                    done += writeCh.write(writeBuf);

                    if (extra != null) {
                        while (off != extra.len) {
                            writeBuf.compact();

                            off = extra.serializeTo(writeBuf, off);

                            writeBuf.flip();
                            done += writeCh.write(writeBuf);
                        }
                    }

                    while (writeBuf.remaining() != 0)
                        done += writeCh.write(writeBuf);

                    assert done == size : done + " " + size;
                }

                assert fileSize == writeCh.size() : fileSize + " " + writeCh.size();

                success = true;
            }
            finally {
                flushMux.release();

                buf.markFlushed(success);

                flippedBuf = null;
            }

            return true;
        }

        /**
         * @param buf Entries buffer.
         * @return {@code true} If we need to flush.
         * @throws GridInterruptedException If interrupted.
         * @throws IOException If failed.
         */
        private boolean needFlush(EntriesBuffer buf) throws GridInterruptedException, IOException {
            return store.writeMode == GridCacheFileLocalStoreWriteMode.SYNC ||
                (store.writeMode == GridCacheFileLocalStoreWriteMode.SYNC_BUFFERED
                && !buf.awaitFlushed(store.writeDelay));
        }

        /**
         * @param e Entry.
         * @return Position in file.
         * @throws GridException If interrupted.
         */
        long write(AbstractFileEntry e) throws GridException {
            try {
                for (;;) {
                    EntriesBuffer buf = curBuf;

                    if (buf == null)
                        throw new GridException("Failed to write to closed file.");

                    if (buf.add(e)) {
                        if (needFlush(buf) && !flush(buf, null, false))
                            buf.awaitFlushed();

                        return e.pos;
                    }

                    if (e.len >= store.writeBufSize) {
                        if (flush(buf, e, false)) // Extra can't be a commit entry.
                            return e.pos;
                    }
                    else
                        flush(buf, null, false);
                }
            }
            catch (IOException ex) {
                throw new GridException(ex);
            }
        }

        /**
         * @param addr Address in file.
         * @return Address with file id in it.
         */
        private long withFileId(long addr) {
            assert (addr >>> fileIdxShift) == 0;

            return (((long)idx) << fileIdxShift) | addr;
        }

        /**
         * @return Time to wait before the next attempt.
         */
        public long tryFlush() {
            EntriesBuffer buf = curBuf;

            if (buf == null)
                return 0;

            try {
                if (buf.isEmpty() || flush(buf, null, false))
                    return store.writeDelay;
            }
            catch (IOException e) {
                throw new GridRuntimeException(e);
            }

            long t = U.microTime();

            return Math.max(buf.created + store.writeDelay - t, 0);
        }

        /**
         * Fsync.
         */
        void fsync() {
            try {
                writeCh.force(false);
            }
            catch (IOException e) {
                store.log.error("Failed to fsync.", e);
            }
        }

        /**
         * Does fsync if needed.
         * @param lastFsync Last fsync time.
         * @return Time to wait before the next attempt.
         */
        public long tryFsync(long lastFsync) {
            EntriesBuffer buf = curBuf;

            if (buf == null)
                return 0;

            if (buf.created <= lastFsync) // Nothing was committed since last fsync.
                return store.fsyncDelay;

            long awaitToFsync = lastFsync + store.fsyncDelay - U.microTime();

            if (awaitToFsync <= 0) {
                fsync();

                awaitToFsync = store.fsyncDelay;
            }

            return awaitToFsync;
        }

        /**
         * @param size Size.
         */
        public void decrementLiveSize(int size) {
            liveSize.addAndGet(-size);
        }

        /**
         * @return Wasted size.
         */
        public long waste() {
            return fileSize - liveSize.get();
        }
    }

    /**
     *
     */
    @SuppressWarnings("PackageVisibleInnerClass")
    static interface DataEntry {
        /**
         * @return Key.
         * @throws GridException if failed.
         */
        byte[] key() throws GridException;

        /**
         * @return Key hash.
         */
        int keyHash();

        /**
         * @return Value or {@code null} if it is a removal entry.
         * @throws GridException if failed.
         */
        @Nullable byte[] value() throws GridException;

        /**
         * @param key Key.
         * @param keyHash Hash code of key.
         * @return {@code true} If key is equal to the given one.
         * @throws GridException If failed.
         */
        boolean keyEquals(byte[] key, int keyHash) throws GridException;

        /**
         * @return Address of entry.
         */
        long position();

        /**
         * @return Size in bytes.
         */
        int size();
    }

    /**
     *
     */
    private enum EntryType {
        /** */
        PUT((short)0xACCA),

        /** */
        REMOVE((short)0x0666),

        /** */
        COMMIT((short)0xFACE),

        /** */
        COMPOUND((short)0xFAFA);

        /** */
        private final short magic;

        /**
         * @param magic Magic.
         */
        EntryType(short magic) {
            this.magic = magic;
        }
    }

    /**
     *
     */
    private abstract static class AbstractFileEntry {
        /** */
        protected long pos;

        /** */
        protected int len;

        /** */
        @GridToStringInclude
        protected AbstractFileEntry prev;

        /**
         * @param len Length in bytes.
         */
        protected AbstractFileEntry(int len) {
            this.len = len;
        }

        /**
         *
         */
        protected AbstractFileEntry() {
            // No-op.
        }

        /**
         * @param buf Buffer.
         */
        abstract void serializeTo(ByteBuffer buf);

        /**
         * @return type.
         */
        abstract EntryType type();

        /**
         * Serialize to the given buffer starting from the given offset of the entry.
         * This is needed for entries not fitting to the buffer.
         *
         * @param buf Buffer.
         * @param off Offset.
         * @return New offset.
         */
        abstract int serializeTo(ByteBuffer buf, int off);
    }

    /**
     *
     */
    private static class CompoundEntry extends AbstractFileEntry {
        /** */
        private final List<AbstractFileEntry> entries;

        /** */
        private int liveSize;

        /**
         * @param cap Capacity.
         */
        CompoundEntry(int cap) {
            entries = new ArrayList<>(cap);
        }

        /**
         * @param e Entry.
         */
        void add(AbstractFileEntry e) {
            assert e.type() != EntryType.COMPOUND;

            if (e.type() != EntryType.REMOVE)
                liveSize += e.len;

            len += e.len;

            entries.add(e);
        }

        /**
         * @param pos Position.
         * @return Entry.
         */
        AbstractFileEntry find(final long pos) {
            long pos0 = this.pos;

            for (int i = 0, size = entries.size(); i < size; i++) {
                AbstractFileEntry e = entries.get(i);

                if (pos0 == pos) {
                    assert e.type() == EntryType.PUT;

                    return e;
                }

                if (pos0 > pos)
                    return null;

                pos0 += e.len;
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override void serializeTo(ByteBuffer buf) {
            for (int i = 0, size = entries.size(); i < size; i++)
                entries.get(i).serializeTo(buf);
        }

        /** {@inheritDoc} */
        @Override EntryType type() {
            return EntryType.COMPOUND;
        }

        /** {@inheritDoc} */
        @Override int serializeTo(ByteBuffer buf, int off) {
            int off0 = off;

            for (int i = 0, size = entries.size(); i < size; i++) {
                AbstractFileEntry e = entries.get(i);

                if (off0 > e.len)
                    off0 -= e.len;
                else if (off0 == 0 && buf.remaining() >= e.len) {
                    e.serializeTo(buf);

                    off += e.len;
                }
                else {
                    int off1 = e.serializeTo(buf, off0);

                    off += off1 - off0;

                    if (off1 != e.len)
                        break;

                    off0 = 0;
                }
            }

            return off;
        }
    }

    /**
     *
     */
    private static class CommitEntry extends AbstractFileEntry {
        /** */
        private final long xid;

        /**
         * @param xid Transaction ID.
         */
        private CommitEntry(long xid) {
            super(10); // magic(2) + xid(8)

            this.xid = xid;
        }

        /** {@inheritDoc} */
        @Override void serializeTo(ByteBuffer buf) {
            buf.putShort(type().magic);
            buf.putLong(xid);
        }

        /** {@inheritDoc} */
        @Override EntryType type() {
            return EntryType.COMMIT;
        }

        /** {@inheritDoc} */
        @Override int serializeTo(ByteBuffer buf, int off) {
            assert off == 0 : off;

            if (buf.remaining() < len)
                return 0;

            serializeTo(buf);

            return len;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CommitEntry.class, this);
        }
    }

    /**
     *
     */
    @SuppressWarnings("PackageVisibleInnerClass")
    static class PutEntry extends AbstractFileEntry implements DataEntry {
        /** magic(2) + keyLen(4) + valLen(4) + keyHash(4) + valHash(4) + xid(8) */
        static final int HEADER_SIZE = 26;

        /** */
        private final long xid;

        /** */
        private final byte[] key;

        /** */
        private final byte[] val;

        /** */
        private final int keyHash;

        /** */
        private final int valHash;

        /**
         * @param key Key.
         * @param keyHash Key hash.
         * @param val Value.
         * @param xid Transaction ID.
         * @param checksum Calculate checksum for value.
         */
        PutEntry(byte[] key, int keyHash, byte[] val, long xid, boolean checksum) {
            super(HEADER_SIZE + key.length + val.length);

            this.xid = xid;
            this.key = key;
            this.val = val;
            this.keyHash = keyHash;
            valHash = checksum ? Arrays.hashCode(val) : 0;
        }

        /** {@inheritDoc} */
        @Override public int keyHash() {
            return keyHash;
        }

        /**
         * @param buf Byte buffer.
         */
        private void writeHeader(ByteBuffer buf) {
            buf.putShort(type().magic);
            buf.putInt(key.length);
            buf.putInt(val.length);
            buf.putInt(keyHash);
            buf.putInt(valHash);
            buf.putLong(xid);
        }

        /** {@inheritDoc} */
        @Override public void serializeTo(ByteBuffer buf) {
            int pos = buf.position();

            writeHeader(buf);

            buf.put(key);
            buf.put(val);

            assert buf.position() - pos == len;
        }

        /** {@inheritDoc} */
        @Override EntryType type() {
            return EntryType.PUT;
        }

        /** {@inheritDoc} */
        @Override public int serializeTo(ByteBuffer buf, int off) {
            assert off == 0 || off >= HEADER_SIZE : off;

            if (off == 0) {
                if (buf.remaining() < HEADER_SIZE)
                    return 0;

                writeHeader(buf);

                if (buf.remaining() == 0)
                    return HEADER_SIZE;
            }
            else
                off -= HEADER_SIZE;

            if (off < key.length) {
                int remainingKey = key.length - off;

                if (remainingKey < buf.remaining()) {
                    buf.put(key, off, remainingKey);

                    off = 0;
                }
                else {
                    int r = buf.remaining();

                    buf.put(key, off, r);

                    return off + HEADER_SIZE + r;
                }
            }
            else
                off -= key.length;

            if (val == null)
                return len;

            assert off <= val.length && off >= 0 : off + " " + val.length;

            int remainingVal = val.length - off;

            if (remainingVal < buf.remaining()) {
                buf.put(val, off, remainingVal);

                return len;
            }

            int r = buf.remaining();

            buf.put(val, off, r);

            return off + HEADER_SIZE + key.length + r;
        }

        /** {@inheritDoc */
        @Override public long position() {
            return pos;
        }

        /** {@inheritDoc} */
        @Override public byte[] key() {
            return key;
        }

        /** {@inheritDoc} */
        @Nullable @Override public byte[] value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public boolean keyEquals(byte[] key, int kHash) {
            return keyHash == kHash && Arrays.equals(this.key, key);
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return len;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PutEntry.class, this);
        }
    }

    /**
     *
     */
    @SuppressWarnings("PackageVisibleInnerClass")
    static class RmvEntry extends AbstractFileEntry {
        /** magic(2) + keyLen(4) + keyHash(4) + xid(8) + fileId(16) */
        static final int HEADER_SIZE = 34;

        /** */
        private final long xid;

        /** */
        private final byte[] key;

        /** */
        private final int keyHash;

        /** */
        private final long fileIdx;

        /** */
        private final long compactIdx;

        /**
         * @param xid Xid.
         * @param key Key.
         * @param keyHash Key hash.
         * @param fileIdx File index.
         * @param compactIdx Compact index.
         */
        protected RmvEntry(long xid, byte[] key, int keyHash, long fileIdx, long compactIdx) {
            super(HEADER_SIZE + key.length);

            this.xid = xid;
            this.key = key;
            this.keyHash = keyHash;
            this.fileIdx = fileIdx;
            this.compactIdx = compactIdx;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RmvEntry.class, this);
        }

        /**
         * @param buf Byte buffer.
         */
        private void writeHeader(ByteBuffer buf) {
            buf.putShort(type().magic);
            buf.putInt(key.length);
            buf.putInt(keyHash);
            buf.putLong(xid);
            buf.putLong(fileIdx);
            buf.putLong(compactIdx);
        }

        /** {@inheritDoc} */
        @Override public void serializeTo(ByteBuffer buf) {
            int pos = buf.position();

            writeHeader(buf);

            buf.put(key);

            assert buf.position() - pos == len;
        }

        /** {@inheritDoc} */
        @Override EntryType type() {
            return EntryType.REMOVE;
        }

        /** {@inheritDoc} */
        @Override int serializeTo(ByteBuffer buf, int off) {
            assert off == 0 || off >= HEADER_SIZE;

            if (off == 0) {
                if (buf.remaining() < HEADER_SIZE)
                    return 0;

                writeHeader(buf);

                if (buf.remaining() == 0)
                    return HEADER_SIZE;
            }
            else
                off -= HEADER_SIZE;

            int remainingKey = key.length - off;

            if (remainingKey < buf.remaining()) {
                buf.put(key, off, remainingKey);

                return len;
            }

            int r = buf.remaining();

            buf.put(key, off, r);

            return off + HEADER_SIZE + r;
        }
    }

    /**
     *
     */
    private static class EntriesBuffer {
        /** */
        private final AtomicInteger remaining = new AtomicInteger();

        /** */
        private final AtomicReference<AbstractFileEntry> head = new AtomicReference<>();

        /** */
        private final long bufPos;

        /** */
        private final int cap;

        /** */
        private final long created = U.microTime();

        /** */
        private final CountDownLatch flushed = new CountDownLatch(1);

        /** */
        private boolean flushSucceeded;

        /**
         * @param bufPos Buffer position.
         * @param cap Buffer capacity in bytes.
         */
        EntriesBuffer(long bufPos, int cap) {
            assert cap > 0;

            this.bufPos = bufPos;
            this.cap = cap;
            remaining.set(cap + 1); // +1 For needed flip (it negates the value).
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(EntriesBuffer.class, this);
        }

        /**
         * Marks this buffer as committed.
         * @param success Committed successfully.
         */
        void markFlushed(boolean success) {
            assert flushed.getCount() == 1;

            flushSucceeded = success;

            flushed.countDown();
        }

        /**
         * @throws IOException If failed.
         */
        private void checkFlushSucceeded() throws IOException {
            if (!flushSucceeded)
                throw new IOException("Write failed in another thread.");
        }

        /**
         * Wait until the buffer become committed.
         * @throws IOException If commit failed.
         * @throws GridInterruptedException if interrupted.
         */
        void awaitFlushed() throws IOException, GridInterruptedException {
            U.await(flushed);

            checkFlushSucceeded();
        }

        /**
         * Wait until the buffer become committed for the given period of time.
         *
         * @param micros Microseconds to wait.
         * @return {@code false} If the waiting time elapsed before the buffer was committed.
         * @throws IOException if failed.
         * @throws GridInterruptedException if interrupted.
         */
        boolean awaitFlushed(long micros) throws GridInterruptedException, IOException {
            boolean res = U.await(flushed, micros, TimeUnit.MICROSECONDS);

            if (!res)
                return false;

            checkFlushSucceeded();

            return true;
        }

        /**
         * @return Occupied size in bytes.
         */
        int size() {
            return cap - remaining();
        }

        /**
         * @return Free space in bytes.
         */
        int remaining() {
            return Math.abs(remaining.get()) - 1;
        }

        /**
         * @param len Number of bytes we want to put in this buffer.
         * @return {@code true} If succeeded.
         */
        private boolean tryAcquire(int len) {
            for (;;) {
                int r = remaining.get();

                if (r <= len) // We can't have 0 in remaining, since it was adjusted to +1.
                    return false;

                if (remaining.compareAndSet(r, r - len))
                    return true;
            }
        }

        /**
         * @param pos Position.
         * @return Entry or {@code null} if not found.
         */
        @Nullable AbstractFileEntry find(long pos) {
            for (AbstractFileEntry e = head.get(); e != null; e = e.prev) {
                if (e.type() == EntryType.COMPOUND) {
                    AbstractFileEntry e0 = ((CompoundEntry)e).find(pos);

                    if (e0 != null)
                        return e0;
                }
                else if (e.pos == pos)
                    return e;

                if (e.pos < pos)
                    break;
            }

            return null;
        }

        /**
         * @param e Entry.
         * @return {@code true} If added.
         */
        boolean add(AbstractFileEntry e) {
            if (!tryAcquire(e.len))
                return false;

            e.pos = bufPos;

            if (head.compareAndSet(null, e))
                return true;

            for (;;) {
                AbstractFileEntry prev = head.get();

                e.pos = prev.pos + prev.len;
                e.prev = prev;

                if (head.compareAndSet(prev, e))
                    return true;
            }
        }

        /**
         * @return {@code true} If empty.
         */
        boolean isEmpty() {
            return remaining() == cap;
        }

        /**
         * @return {@code true} If we flipped the buffer.
         */
        boolean flip() {
            for (;;) {
                int r = remaining.get();

                assert r != 0;

                if (r < 0)
                    return false;

                if (remaining.compareAndSet(r, -r))
                    return true;
            }
        }

        /**
         * @param buf Buffer.
         */
        public void serializeTo(ByteBuffer buf) {
            assert buf.position() == 0;

            AbstractFileEntry e = head.get();

            if (e == null)
                return; // Nothing to serialize.

            int pos = -1;

            do {
                buf.position((int)(e.pos - bufPos));

                e.serializeTo(buf);

                if (pos == -1) // Remember end position.
                    pos = buf.position();

                e = e.prev;
            }
            while(e != null);

            buf.position(pos);
        }

        /**
         * @return Accumulated useful size.
         */
        public int liveSize() {
            int res = 0;

            for (AbstractFileEntry e = head.get(); e != null; e = e.prev) {
                switch (e.type()) {
                    case REMOVE:
                        break;

                    case COMPOUND:
                        res += ((CompoundEntry)e).liveSize;

                        break;

                    default:
                        res += e.len;
                }
            }

            return res;
        }

        /**
         * Await all arrived.
         */
        public void awaitAllArrived() {
            if (isEmpty())
                return;

            int r = remaining();

            for (;;) { // Wait last entry to arrive.
                AbstractFileEntry e = head.get();

                if (e != null && e.pos - bufPos + e.len == cap - r)
                    return;
            }
        }
    }
}
