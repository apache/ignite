/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.offheap.unsafe;

import org.gridgain.grid.util.offheap.unsafe.reproducing.*;

import java.util.*;

/**
 * Off-heap map based on {@code Unsafe} implementation.
 */
public class GridUnsafeMap0 {
    /** Segments. */
    private final Segment seg;

    /** Total memory. */
    private final GridUnsafeMemory mem;

    /** Striped LRU policy. */
    private final GridUnsafeLru lru;

    /** LRU poller. */
    private final GridUnsafeLruPoller lruPoller;

    @SuppressWarnings("unchecked")
    public GridUnsafeMap0(short lruStripes) throws Exception {
        mem = new GridUnsafeMemory(10000);

        lru = new GridUnsafeLru(lruStripes, mem);

        // Find power-of-two sizes best matching arguments
        seg = new Segment<>();

        binAddr = mem.allocateSystem(800, true);

        lruPoller = new GridUnsafeLruPoller() {
            @Override public void lruPoll(int size) {
                int left = size;

                while (left > 0) {
                    // Pre-poll outside of lock.
                    long qAddr = lru.prePoll();

                    if (qAddr == 0)
                        return; // LRU is empty.

                    short order = lru.order(qAddr);

                    int released = seg.freeSpace(order, qAddr);

                    if (released == 0)
                        return;

                    left -= released;
                }
            }
        };
    }

    /** {@inheritDoc} */
    public boolean put(byte[] keyBytes, byte[] valBytes) {
        boolean isNew = true;

        boolean poll = false;

        int size = 0;

        int relSize = 0;
        long relAddr = 0;

        try {
            long first = mem.readLong(binAddr);

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

            mem.writeLong(binAddr, addr);

            if (isNew)
                qAddr = lru.offer(0, addr, 1);

            lru.touch(qAddr, addr);

            Entry.write(addr, 1, keyBytes, valBytes, qAddr, first, mem);

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

    private long binAddr;

    /**
     * Segment.
     */
    private class Segment<K> {
        @SuppressWarnings({"TooBroadScope", "AssertWithSideEffects"})
        private int freeSpace(short order, long qAddr) {
            int relSize = 0;
            long relAddr = 0;

            try {
                // Read LRU queue node inside of the lock.
                long addr = lru.entry(order, qAddr);

                if (addr != 0) {
                    long first = mem.readLong(binAddr);

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
                                    mem.writeLong(binAddr, 0L);
                                else
                                    mem.writeLong(binAddr, next);
                            }

                            byte[] valBytes = UnsafeWrapper.faultyMethod(cur);

                            // TODO GG-8123: Dump on error.
                            if (valBytes[0] != 114)
                                System.out.println("PROBLEM: " + Arrays.toString(valBytes));

                            relSize = Entry.size(cur, mem);
                            relAddr = cur;
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
