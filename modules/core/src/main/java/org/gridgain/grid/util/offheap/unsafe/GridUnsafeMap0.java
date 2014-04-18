/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.offheap.unsafe;

import org.gridgain.grid.util.offheap.unsafe.reproducing.*;
import org.jdk8.backport.*;

import java.util.*;

/**
 * Off-heap map based on {@code Unsafe} implementation.
 */
public class GridUnsafeMap0<K> {
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
    private final Segment seg;

    /** Total memory. */
    private final GridUnsafeMemory mem;

    /** Striped LRU policy. */
    private final GridUnsafeLru lru;

    /** Total entry count. */
    private final LongAdder totalCnt;

    /** LRU poller. */
    private final GridUnsafeLruPoller lruPoller;

    @SuppressWarnings("unchecked")
    public GridUnsafeMap0(float load, short lruStripes) throws Exception {
        this.load = load;

        part = 0;

        mem = new GridUnsafeMemory(10000);

        lru = new GridUnsafeLru(lruStripes, mem);

        totalCnt = new LongAdder();

        // Find power-of-two sizes best matching arguments
        seg = new Segment<>(100);

        lruPoller = new GridUnsafeLruPoller() {
            @Override public void lruPoll(int size) {
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

    /** {@inheritDoc} */
    public boolean put(int hash, byte[] keyBytes, byte[] valBytes) {
        return seg.put(hash, keyBytes, valBytes);
    }

    /**
     * Frees space by polling entries from LRU queue.
     *
     * @param qAddr Queue node address.
     */
    int freeSpace(short order, long qAddr) {
        int hash = lru.hash(order, qAddr);

        return seg.freeSpace(hash, order, qAddr);
    }

    /**
     * Segment.
     */
    private class Segment<K> {
        /** Capacity. */
        private volatile long cap;

        /** Memory capacity. */
        private volatile long memCap;

        /** Table pointer. */
        private volatile long tblAddr;

        /** Threshold. */
        private long threshold;

        /**
         * @param cap Capacity.
         */
        private Segment(long cap) {
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
         * Frees space by polling entries from LRU queue.
         *
         * @param hash Hash code.
         * @param order Queue stripe order.
         * @param qAddr Queue address.
         */
        @SuppressWarnings({"TooBroadScope", "AssertWithSideEffects"})
        private int freeSpace(int hash, short order, long qAddr) {
            int relSize = 0;
            long relAddr = 0;

            long binAddr = binAddress(hash);

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

                            byte[] valBytes = UnsafeWrapper.faultyMethod(cur);

                            // TODO GG-8123: Dump on error.
                            if (valBytes[0] != 114)
                                System.out.println("PROBLEM: " + Arrays.toString(valBytes));

                            relSize = Entry.size(cur, mem);
                            relAddr = cur;

                            totalCnt.decrement();
                        }
                    }
                }

                // Remove from LRU.
                lru.poll(qAddr);
            }
            finally {
                // Remove current mapping outside of lock.
                mem.release(relAddr, relSize);
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

            long binAddr = binAddress(hash);

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
                    totalCnt.increment();

                    qAddr = lru == null ? 0 : lru.offer(part, addr, hash);
                }
                else if (lru != null)
                    lru.touch(qAddr, addr);

                Entry.write(addr, hash, keyBytes, valBytes, qAddr, first, mem);

                return isNew;
            }
            finally {
                // Release memory outside of lock.
                if (relAddr != 0)
                    mem.release(relAddr, relSize);

                if (poll)
                    lruPoller.lruPoll(size);
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
        static void writeValueBytes(long ptr, int keyLen, byte[] valBytes, GridUnsafeMemory mem) {
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


            mem.writeBytes(ptr + (long)HEADER, keyBytes);
            mem.writeBytes(ptr + HEADER + keyBytes.length, valBytes);
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
