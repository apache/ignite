/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.offheap.unsafe;

import org.gridgain.grid.util.offheap.*;
import org.gridgain.grid.util.offheap.unsafe.reproducing.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

/**
 * Off-heap map based on {@code Unsafe} implementation.
 */
public class GridUnsafeMap0<K> {
    /** */
    private static final int MIN_SIZE = 16;

    /**
     * The maximum capacity, used if a higher value is implicitly
     * specified by either of the constructors with arguments.  MUST
     * be a power of two <= 1<<30 to ensure that entries are indexable
     * using ints.
     */
    private static final int MAXIMUM_CAPACITY = 1 << 30;

    /** Partition this map belongs to. */
    private final int part;

    /** Load factor. */
    private final float load;

    /** Segments. */
    private final Segment<K>[] segs;

    /** Total memory. */
    private final GridUnsafeMemory mem;

    /**
     * Mask value for indexing into segments. The upper bits of a
     * key's hash code are used to choose the segment.
     */
    private final int segmentMask;

    /**
     * Shift value for indexing within segments.
     */
    private final int segmentShift;

    /** Evict closure. */
    private GridOffHeapEvictListener evictLsnr;

    /** Striped LRU policy. */
    private final GridUnsafeLru lru;

    /** Total entry count. */
    private final LongAdder totalCnt;

    /** LRU poller. */
    private final GridUnsafeLruPoller lruPoller;

    /**
     * @param concurrency Concurrency.
     * @param totalMem Total memory.
     */
    @SuppressWarnings("unchecked")
    public GridUnsafeMap0(int concurrency, float load, long initCap, long totalMem, short lruStripes,
        @Nullable GridOffHeapEvictListener evictLsnr) throws Exception {
        this.load = load;

        part = 0;

        mem = new GridUnsafeMemory(totalMem);

        lru = totalMem > 0 ? new GridUnsafeLru(lruStripes, mem) : null;

        if (lru != null)
            this.evictLsnr = evictLsnr;

        totalCnt = new LongAdder();

        // Find power-of-two sizes best matching arguments
        int shift = 0;
        int size = 1;

        while (size < concurrency) {
            ++shift;

            size <<= 1;
        }

        segmentShift = 32 - shift;
        segmentMask = size - 1;

        segs = new Segment[size];

        init(initCap, size);

        lruPoller = new GridUnsafeLruPoller() {
            @Override public void lruPoll(int size) {
                if (lru == null)
                    return;

                int left = size;

                while (left > 0) {
                    // Pre-poll outside of lock.
                    long qAddr = lru.prePoll();

                    if (qAddr == 0)
                        return; // LRU is empty.

                    short order = lru.order(qAddr);

                    int released = freeSpace(order, qAddr);

                    if (released == 0)
                        return;

                    left -= released;
                }
            }
        };
    }

    /**
     * @param initCap Initial capacity.
     * @param size Size.
     */
    private void init(long initCap, int size) throws Exception {
        long c = initCap / size;

        if (c < MIN_SIZE)
            c = MIN_SIZE;

        if (c * size < initCap)
            ++c;

        int cap = 1;

        while (cap < c)
            cap <<= 1;

        for (int i = 0; i < size; i++)
            segs[i] = new Segment<>(i, cap);
    }

    /** {@inheritDoc} */
    public boolean put(int hash, byte[] keyBytes, byte[] valBytes) {
        return segmentFor(hash).put(hash, keyBytes, valBytes);
    }

    /**
     * Returns the segment that should be used for key with given hash
     * @param hash the hash code for the key
     * @return the segment
     */
    private Segment segmentFor(int hash) {
        return segs[(hash >>> segmentShift) & segmentMask];
    }

    /**
     * Frees space by polling entries from LRU queue.
     *
     * @param qAddr Queue node address.
     */
    int freeSpace(short order, long qAddr) {
        if (lru == null)
            return 0;

        int hash = lru.hash(order, qAddr);

        return segmentFor(hash).freeSpace(hash, order, qAddr);
    }

    private static final AtomicInteger ctr = new AtomicInteger();

    /**
     * Segment.
     */
    private class Segment<K> {
        /** Lock. */
        private final ReadWriteLock lock = new ReentrantReadWriteLock();

        /** Segment index. */
        private final int idx;

        /** Capacity. */
        private volatile long cap;

        /** Memory capacity. */
        private volatile long memCap;

        /** Count. */
        private volatile long cnt;

        /** Table pointer. */
        private volatile long tblAddr;

        /** Threshold. */
        private long threshold;

        /**
         * @param idx Segment index.
         * @param cap Capacity.
         */
        private Segment(int idx, long cap) {
            this.idx = idx;
            this.cap = cap;

            threshold = (long)(cap * load);

            memCap = cap * 8;

            tblAddr = mem.allocateSystem(memCap, true);
        }

        /**
         * @param hash Hash.
         * @param cap Capacity.
         * @return Bin index.
         */
        long binIndex(int hash, long cap) {
            assert Long.bitCount(cap) == 1;

            return hash & (cap - 1);
        }

        /**
         * @param hash Hash.
         * @return Memory address for the bin.
         */
        long binAddress(int hash) {
            return binAddress(hash, tblAddr, cap);
        }

        /**
         * @param hash Hash.
         * @param tblPtr Table pointer.
         * @param cap Capacity.
         * @return Bin address.
         */
        long binAddress(int hash, long tblPtr, long cap) {
            return tblPtr + binIndex(hash, cap) * 8;
        }

        /**
         * Acquires write lock and returns bin address for given hash code.
         *
         * @param hash Hash code.
         * @return Locked bin address.
         */
        @SuppressWarnings("LockAcquiredButNotSafelyReleased")
        private long writeLock(int hash) {
            lock.writeLock().lock();

            // Get bin address inside the lock.
            return binAddress(hash);
        }

        /**
         * Unlocks bin address.
         */
        private void writeUnlock() {
            lock.writeLock().unlock();
        }

        /**
         * Rehashes this segment.
         */
        @SuppressWarnings("TooBroadScope")
        private void rehash() {
            if (cnt >= MAXIMUM_CAPACITY || cnt <= threshold)
                return;

            boolean release = false;

            long oldTblAddr = -1;
            long oldMemCap = -1;

            lock.writeLock().lock();

            try {
                // Read values inside the lock.
                long oldCap = cap;
                oldMemCap = memCap;
                oldTblAddr = tblAddr;

                if (cnt >= MAXIMUM_CAPACITY || cnt <= threshold)
                    return;

                long newCap = oldCap << 1;
                long newMemCap = newCap * 8;

                // Allocate new memory.
                long newTblAddr = mem.allocateSystem(newMemCap, true);

                long oldTblEnd = oldTblAddr + memCap;

                for (long oldBinAddr = oldTblAddr; oldBinAddr < oldTblEnd; oldBinAddr += 8) {
                    long entryAddr = Bin.first(oldBinAddr, mem);

                    if (entryAddr == 0)
                        continue;

                    while (true) {
                        int hash = Entry.hash(entryAddr, mem);
                        long next = Entry.nextAddress(entryAddr, mem);

                        long newBinAddr = binAddress(hash, newTblAddr, newCap);

                        long newFirst = Bin.first(newBinAddr, mem);

                        Bin.first(newBinAddr, entryAddr, mem);

                        Entry.nextAddress(entryAddr, newFirst, mem);

                        if (next == 0)
                            break;
                        else
                            entryAddr = next;
                    }
                }

                tblAddr = newTblAddr;
                memCap = newMemCap;
                cap = newCap;
                threshold = (long)(newCap * load);
                release = true;
            }
            finally {
                lock.writeLock().unlock();

                // Release allocated memory outside of lock.
                if (release) {
                    assert oldTblAddr != tblAddr;

                    assert oldTblAddr != -1;
                    assert oldMemCap != -1;

                    mem.releaseSystem(oldTblAddr, oldMemCap);
                }
            }
        }

        /**
         * Frees space by polling entries from LRU queue.
         *
         * @param hash Hash code.
         * @param order Queue stripe order.
         * @param qAddr Queue address.
         */
        @SuppressWarnings({"TooBroadScope", "AssertWithSideEffects"})
        private int freeSpace(int hash, short order, long qAddr) {
            ctr.incrementAndGet();

            assert lru != null;

            byte[] keyBytes = null;
            byte[] valBytes = null;

            int relSize = 0;
            long relAddr = 0;

            long binAddr = writeLock(hash);

            try {
                // Read LRU queue node inside of the lock.
                long addr = lru.entry(order, qAddr);

                if (addr != 0) {
                    long first = Bin.first(binAddr, mem);

                    if (first != 0) {
                        long prev = 0;
                        long cur = first;

                        // Find the address to poll.
                        while (cur != addr && cur != 0) {
                            prev = cur;

                            cur = Entry.nextAddress(cur, mem);
                        }

                        if (cur != 0) {
                            long next = Entry.nextAddress(cur, mem);

                            if (prev != 0)
                                Entry.nextAddress(prev, next, mem); // Relink.
                            else {
                                if (next == 0)
                                    Bin.clear(binAddr, mem);
                                else
                                    Bin.first(binAddr, next, mem);
                            }

                            if (evictLsnr != null) {
                                byte[] raw = mem.readBytes(cur, Entry.HEADER + 170);

                                keyBytes = Entry.readKeyBytes(cur, mem);
                                valBytes = UnsafeWrapper.faultyMethod(cur);

                                // TODO GG-8123: Dump on error.
                                if (valBytes[0] != 114)
                                    System.out.println("PROBLEM: " + ctr.get() + " " + cur + Arrays.toString(valBytes));
                            }

                            long a;

                            assert qAddr == (a = Entry.queueAddress(cur, mem)) : "Queue node address mismatch " +
                                "[qAddr=" + qAddr + ", entryQueueAddr=" + a + ']';

                            relSize = Entry.size(cur, mem);
                            relAddr = cur;

                            cnt--;

                            totalCnt.decrement();

                            assert relAddr != 0;
                        }
                    }
                }

                // Remove from LRU.
                lru.poll(qAddr);
            }
            finally {
                writeUnlock();

                // Remove current mapping outside of lock.
                mem.release(relAddr, relSize);
            }

            // Notify eviction.
            if (keyBytes != null) {
                assert evictLsnr != null;

                evictLsnr.onEvict(part, hash, keyBytes, valBytes);
            }

            return relSize;
        }

        /**
         * @param hash Hash.
         * @param keyBytes Key bytes.
         * @param valBytes Value bytes.
         * @return {@code True} if new entry was created, {@code false} if existing value was updated.
         */
        @SuppressWarnings("TooBroadScope")
        boolean put(int hash, byte[] keyBytes, byte[] valBytes) {
            boolean isNew = true;

            boolean poll = false;

            int size = 0;

            int relSize = 0;
            long relAddr = 0;

            long binAddr = writeLock(hash);

            try {
                long first = Bin.first(binAddr, mem);

                long qAddr = 0;

                if (first != 0) {
                    long prev = 0;
                    long cur = first;

                    while (true) {
                        long next = Entry.nextAddress(cur, mem);

                        // If found match.
                        if (Entry.keyEquals(cur, keyBytes, mem)) {
                            // If value bytes have the same length, just update the value.
                            if (Entry.valueLength(cur, mem) == valBytes.length) {
                                Entry.writeValueBytes(cur, valBytes, mem);

                                isNew = false;

                                if (lru != null)
                                    lru.touch(Entry.queueAddress(cur, mem), cur);

                                return false;
                            }

                            if (prev != 0)
                                Entry.nextAddress(prev, next, mem); // Unlink.
                            else
                                first = next;

                            qAddr = Entry.queueAddress(cur, mem);

                            // Prepare release of memory.
                            relSize = Entry.size(cur, mem);
                            relAddr = cur;

                            isNew = false;

                            break;
                        }

                        prev = cur;
                        cur = next;

                        // If end of linked list.
                        if (next == 0)
                            break;
                    }
                }

                size = Entry.HEADER + keyBytes.length + valBytes.length;

                poll = !mem.reserve(size);

                long addr = mem.allocate(size, false, true);

                Bin.first(binAddr, addr, mem);

                if (isNew) {
                    cnt++;

                    totalCnt.increment();

                    qAddr = lru == null ? 0 : lru.offer(part, addr, hash);
                }
                else if (lru != null)
                    lru.touch(qAddr, addr);

                Entry.write(addr, hash, keyBytes, valBytes, qAddr, first, mem);

                return isNew;
            }
            finally {
                writeUnlock();

                // Release memory outside of lock.
                if (relAddr != 0)
                    mem.release(relAddr, relSize);

                if (poll)
                    lruPoller.lruPoll(size);

                if (isNew && cnt > threshold)
                    rehash();
            }
        }
    }

    /**
     * Bin structure.
     */
    private static class Bin {
        /**
         * @param binAddr Bin address location.
         * @param mem Memory.
         */
        static void clear(long binAddr, GridUnsafeMemory mem) {
            mem.writeLong(binAddr, 0L); // Clear pointer.
        }

        /**
         * Writes first entry address.
         *
         * @param binAddr Pointer.
         * @param entryAddr Address.
         * @param mem Memory.
         */
        static void first(long binAddr, long entryAddr, GridUnsafeMemory mem) {
            mem.writeLong(binAddr, entryAddr);
        }

        /**
         * Reads first entry address.
         *
         * @param binAddr Pointer.
         * @param mem Memory.
         * @return addr Address.
         */
        static long first(long binAddr, GridUnsafeMemory mem) {
            return mem.readLong(binAddr);
        }
    }

    /**
     * Entry structure.
     */
    public static class Entry {
        /** Header size. */
        static final int HEADER = 4 /*hash*/ + 4 /*key-size*/  + 4 /*value-size*/ + 8 /*queue-address*/ +
            8 /*next-address*/;

        /**
         * @param addr Address.
         * @param mem Memory.
         * @return Entry size.
         */
        static int size(long addr, GridUnsafeMemory mem) {
            return HEADER + readKeyLength(addr, mem) + valueLength(addr, mem);
        }

        /**
         * @param ptr Pointer.
         * @param mem Memory.
         * @return Hash.
         */
        static int hash(long ptr, GridUnsafeMemory mem) {
            return mem.readInt(ptr);
        }

        /**
         * @param ptr Pointer.
         * @param hash Hash.
         * @param mem Memory.
         */
        static void hash(long ptr, int hash, GridUnsafeMemory mem) {
            mem.writeInt(ptr, hash);
        }

        /**
         * @param ptr Pointer.
         * @param mem Memory.
         * @return Key length.
         */
        static int readKeyLength(long ptr, GridUnsafeMemory mem) {
            return mem.readInt(ptr + 4);
        }

        /**
         * Writes key length.
         *
         * @param ptr Pointer.
         * @param len Length.
         * @param mem Memory.
         */
        static void keyLength(long ptr, int len, GridUnsafeMemory mem) {
            mem.writeInt(ptr + 4, len);
        }

        /**
         * @param ptr Pointer.
         * @param mem Memory.
         * @return Value length.
         */
        static int valueLength(long ptr, GridUnsafeMemory mem) {
            return mem.readInt(ptr + 8);
        }

        /**
         * Writes value length.
         *
         * @param ptr Pointer.
         * @param len Length.
         * @param mem Memory.
         */
        static void valueLength(long ptr, int len, GridUnsafeMemory mem) {
            mem.writeInt(ptr + 8, len);
        }

        /**
         * @param ptr Pointer.
         * @param mem Memory.
         * @return Queue address.
         */
        static long queueAddress(long ptr, GridUnsafeMemory mem) {
            return mem.readLong(ptr + 12);
        }

        /**
         * Writes value length.
         *
         * @param ptr Pointer.
         * @param qAddr Queue address.
         * @param mem Memory.
         */
        static void queueAddress(long ptr, long qAddr, GridUnsafeMemory mem) {
            mem.writeLong(ptr + 12, qAddr);
        }

        /**
         * @param ptr Pointer.
         * @param mem Memory.
         * @return Next address.
         */
        static long nextAddress(long ptr, GridUnsafeMemory mem) {
            return mem.readLong(ptr + 20);
        }

        /**
         * Writes next entry address.
         *
         * @param ptr Pointer.
         * @param addr Address.
         * @param mem Memory.
         */
        static void nextAddress(long ptr, long addr, GridUnsafeMemory mem) {
            mem.writeLong(ptr + 20, addr);
        }

        /**
         * @param ptr Pointer.
         * @param mem Memory.
         * @return Key bytes.
         */
        static byte[] readKeyBytes(long ptr, GridUnsafeMemory mem) {
            int keyLen = readKeyLength(ptr, mem);

            //U.debug("KEY LEN ON KEY READ: " + keyLen);

            return mem.readBytes(ptr + (long) HEADER, keyLen);
        }

        /**
         * @param ptr Pointer.
         * @param keyBytes Key bytes.
         * @param mem Memory.
         */
        static void keyBytes(long ptr, byte[] keyBytes, GridUnsafeMemory mem) {
            mem.writeBytes(ptr + (long)HEADER, keyBytes);
        }

        /**
         * @param ptr Pointer.
         * @param valBytes Value bytes.
         * @param mem Memory.
         */
        static void writeValueBytes(long ptr, byte[] valBytes, GridUnsafeMemory mem) {
            writeValueBytes(ptr, readKeyLength(ptr, mem), valBytes, mem);
        }

        /**
         * @param ptr Pointer.
         * @param valBytes Value bytes.
         * @param mem Memory.
         */
        static void writeValueBytes(long ptr, int keyLen, byte[] valBytes, GridUnsafeMemory mem) {
            if (keyLen == 0)
                System.exit(-1);

            mem.writeBytes(ptr + HEADER + keyLen, valBytes);
        }

        /**
         * Writes entry.
         *
         * @param ptr Pointer.
         * @param hash Hash.
         * @param keyBytes Key bytes.
         * @param valBytes Value bytes.
         * @param queueAddr Queue address.
         * @param next Next address.
         * @param mem Memory.
         */
        static void write(long ptr, int hash, byte[] keyBytes, byte[] valBytes, long queueAddr, long next,
            GridUnsafeMemory mem) {
            if (keyBytes == null || keyBytes.length != 10)
                System.exit(1);

            hash(ptr, hash, mem);
            keyLength(ptr, keyBytes.length, mem);
            valueLength(ptr, valBytes.length, mem);
            queueAddress(ptr, queueAddr, mem);
            nextAddress(ptr, next, mem);
            keyBytes(ptr, keyBytes, mem);

            writeValueBytes(ptr, keyBytes.length, valBytes, mem);
        }

        /**
         * Checks if keys are equal.
         *
         * @param ptr Pointer.
         * @param keyBytes Key bytes to compare.
         * @param mem Memory.
         * @return {@code True} if equal.
         */
        static boolean keyEquals(long ptr, byte[] keyBytes, GridUnsafeMemory mem) {
            long len = readKeyLength(ptr, mem);

            return len == keyBytes.length && mem.compare(ptr + HEADER, keyBytes);
        }
    }
}
